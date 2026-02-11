/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.pybuilder

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.pybuilder.PythonReflectionTextUtils.{
  countOccurrences,
  extractContexts,
  formatThrowable,
  truncateBlock
}
import org.apache.texera.amber.pybuilder.PythonReflectionUtils.{
  Finding,
  RawInvalidTextResult,
  TypeEnv
}

import java.lang.reflect._
import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Try
//IMPORTANT ENABLE EXISTENTIALs
import scala.language.existentials

object DescriptorChecker {
  final case class CheckResult(findings: Seq[Finding], code: Option[String])
}

/**
  * Validates a [[PythonOperatorDescriptor]] by instantiating it and attempting to generate Python code.
  *
  * What it does (high level):
  *  1) Instantiates the descriptor (supports Scala object descriptors via MODULE$).
  *  2) Best-effort initializes @JsonProperty fields using defaults and "required" semantics.
  *  3) Inject raw invalid string-typed @JsonProperty fields (and string containers) to detect invalid code.
  *  4) Captures stdout/stderr and exceptions from generatePythonCode() and reports them as findings.
  *
  * Generic-awareness:
  *  - Tracks a best-effort TypeEnv (TypeVariable -> Type) per instantiated object (identity-based) so that
  *    defaults/injection can reason about element types for generic collections.
  *
  * Note (EN): The idea is to "touch" the object as little as possible, but enough to reveal common problems
  * (null required fields, missing defaults, raw text leak, prints to stdout/stderr).
  */
final class DescriptorChecker(private val rawInvalidText: String, private val maxDepth: Int) {

  // Carry env per instantiated object (Identity semantics)
  private val envByObj = new util.IdentityHashMap[AnyRef, TypeEnv]()
  import DescriptorChecker.CheckResult

  /** Convenience wrapper that only returns findings (drops generated code). */
  def check(descriptorClass: Class[_ <: PythonOperatorDescriptor]): Seq[Finding] =
    checkWithCode(descriptorClass).findings

  /**
    * Runs the full validation pipeline and returns both findings and (if generated) Python code.
    *
    * Important: This method tries hard to continue even if parts fail (best-effort strategy),
    * so you can see multiple issues in a single run instead of failing fast on the first problem.
    */
  def checkWithCode(descriptorClass: Class[_ <: PythonOperatorDescriptor]): CheckResult = {
    instantiateDescriptor(descriptorClass) match {
      case Left(instantiateFailureReason) =>
        CheckResult(
          Seq(Finding(descriptorClass.getName, "instantiate", instantiateFailureReason)),
          None
        )

      case Right(descriptorInstance) =>
        val findingsBuffer = mutable.ArrayBuffer.empty[Finding]

        // Seed env for the root descriptor instance (used by later generic-aware routines)
        envByObj.put(descriptorInstance, computeEnvFromConcreteClass(descriptorInstance.getClass))

        // 0) Fill required/defaulted props (deep)
        bestEffortFillJsonPropertyDefaults(descriptorInstance, maxDepth)

        // 1) Raw Invalid strings (deep)
        val rawInvalidTextingResult =
          rawInvalidTextJsonPropertyStringsDeep(descriptorInstance, rawInvalidText, maxDepth)
        if (rawInvalidTextingResult.failed.nonEmpty) {
          findingsBuffer += Finding(
            descriptorClass.getName,
            "injection-failure",
            s"Could not rawInvalidText some @JsonProperty members: ${rawInvalidTextingResult.failed.mkString(", ")}"
          )
        }

        // 2) Capture stdout/stderr + exceptions during codegen
        val consoleCapture = PythonConsoleCapture.captureOutErr {
          Try(descriptorInstance.generatePythonCode())
        }

        val generatedCodeTry = consoleCapture.value
        val generatedCodeOpt = generatedCodeTry.toOption
        val capturedStdout = consoleCapture.out.trim
        val capturedStderr = consoleCapture.err.trim

        if (capturedStdout.nonEmpty) {
          findingsBuffer += Finding(
            descriptorClass.getName,
            "stdout",
            s"generatePythonCode printed to stdout:\n${truncateBlock(capturedStdout, maxLines = 30, maxChars = 4000)}"
          )
        }
        if (capturedStderr.nonEmpty) {
          findingsBuffer += Finding(
            descriptorClass.getName,
            "stderr",
            s"generatePythonCode printed to stderr:\n${truncateBlock(capturedStderr, maxLines = 30, maxChars = 4000)}"
          )
        }

        generatedCodeTry.failed.toOption.foreach { thrown =>
          findingsBuffer += Finding(descriptorClass.getName, "exception", formatThrowable(thrown))
        }

        // 3) Raw invalid string leakage check: did the rawInvalidText marker appear in generated Python?
        generatedCodeOpt.foreach { generatedCode =>
          val rawInvalidTextHitCount = countOccurrences(generatedCode, rawInvalidText)
          if (rawInvalidTextHitCount > 0) {
            val rawInvalidTextContexts =
              extractContexts(generatedCode, rawInvalidText, radius = 160, maxContexts = 2)
                .map(_.replace("\n", "\\n"))
                .mkString("\n  - ...", "...\n  - ...", "...")

            findingsBuffer += Finding(
              descriptorClass.getName,
              "raw-invalid-text-leak",
              s"""Generated Python contains rawInvalidText '$rawInvalidText' ($rawInvalidTextHitCount occurrence(s))
                 |rawInvalidTexted members: ${if (rawInvalidTextingResult.changed.isEmpty)
                "(none found)"
              else rawInvalidTextingResult.changed.mkString(", ")}
                 |contexts:
                 |$rawInvalidTextContexts""".stripMargin
            )
          }
        }

        CheckResult(findingsBuffer.toSeq, generatedCodeOpt)
    }
  }

  /**
    * Instantiates a descriptor:
    *  - Scala object: fetches MODULE$
    *  - Regular class: uses an accessible no-arg constructor
    */
  private def instantiateDescriptor(
      descriptorClass: Class[_ <: PythonOperatorDescriptor]
  ): Either[String, PythonOperatorDescriptor] = {
    val scalaModuleFieldOpt: Option[Field] =
      Try(descriptorClass.getField("MODULE$")).toOption
        .orElse(Try(descriptorClass.getDeclaredField("MODULE$")).toOption)

    scalaModuleFieldOpt match {
      case Some(scalaModuleField) =>
        Try {
          scalaModuleField.setAccessible(true)
          scalaModuleField.get(null).asInstanceOf[PythonOperatorDescriptor]
        }.toEither.left.map(thrown =>
          s"cannot access Scala object MODULE $scalaModuleFieldOpt: ${thrown.getClass.getName}: ${Option(thrown.getMessage)
            .getOrElse("")}"
        )

      case None =>
        Try {
          val noArgConstructor = descriptorClass.getDeclaredConstructor()
          noArgConstructor.setAccessible(true)
          noArgConstructor.newInstance().asInstanceOf[PythonOperatorDescriptor]
        }.toEither.left.map(_ =>
          "cannot instantiate (needs an accessible no-arg constructor or must be a Scala object)"
        )
    }
  }

  // ------------------------------------------------------------
  // Generic type resolution (TypeEnv)
  // ------------------------------------------------------------

  private final case class SimpleParameterizedType(raw: Type, args: scala.Array[Type], owner: Type)
      extends ParameterizedType {
    override def getRawType: Type = raw
    override def getActualTypeArguments: scala.Array[Type] = args.clone()
    override def getOwnerType: Type = owner
  }

  /**
    * Builds a best-effort mapping of type variables to concrete types by walking:
    *  - generic superclass
    *  - generic interfaces
    * recursively up the inheritance chain.
    */
  private def computeEnvFromConcreteClass(concreteClass: Class[_]): TypeEnv = {
    val typeVarBindings = mutable.Map.empty[TypeVariable[_], Type]
    val visitedTypes = mutable.Set.empty[Type]

    def resolveInCollectedEnv(unresolvedType: Type): Type =
      resolveType(unresolvedType, typeVarBindings.toMap)

    def traverseType(nextType: Type): Unit = {
      if (nextType == null || visitedTypes.contains(nextType)) return
      visitedTypes += nextType

      nextType match {
        case parameterizedType: ParameterizedType =>
          val rawClassOpt = typeToClass(parameterizedType.getRawType)
          rawClassOpt.foreach { rawClass =>
            val rawTypeVariables = rawClass.getTypeParameters
            val typeArguments = parameterizedType.getActualTypeArguments
            rawTypeVariables.zipAll(typeArguments, null, null).foreach {
              case (typeVar, typeArg) =>
                if (typeVar != null && typeArg != null)
                  typeVarBindings(typeVar) = resolveInCollectedEnv(typeArg)
            }
          }
          rawClassOpt.foreach(traverseClass)

        case rawClass: Class[_] =>
          traverseClass(rawClass)

        case _ =>
          ()
      }
    }

    def traverseClass(currentClass: Class[_]): Unit = {
      if (currentClass == null || currentClass == classOf[Object]) return
      traverseType(currentClass.getGenericSuperclass)
      currentClass.getGenericInterfaces.foreach(traverseType)
      traverseClass(currentClass.getSuperclass)
    }

    traverseClass(concreteClass)
    typeVarBindings.toMap
  }

  private def resolveType(unresolvedType: Type, typeEnv: TypeEnv): Type =
    unresolvedType match {
      case typeVar: TypeVariable[_] =>
        typeEnv.get(typeVar) match {
          case Some(resolvedBinding) => resolveType(resolvedBinding, typeEnv)
          case None =>
            typeVar.getBounds.headOption
              .map(bound => resolveType(bound, typeEnv))
              .getOrElse(typeVar)
        }

      case wildcardType: WildcardType =>
        wildcardType.getUpperBounds.headOption
          .map(bound => resolveType(bound, typeEnv))
          .getOrElse(wildcardType)

      case genericArrayType: GenericArrayType =>
        val resolvedComponentType = resolveType(genericArrayType.getGenericComponentType, typeEnv)
        typeToClass(resolvedComponentType)
          .map(componentClass =>
            java.lang.reflect.Array.newInstance(componentClass, 0).getClass.asInstanceOf[Type]
          )
          .getOrElse(genericArrayType)

      case parameterizedType: ParameterizedType =>
        val resolvedRawType = resolveType(parameterizedType.getRawType, typeEnv)
        val resolvedOwnerType =
          Option(parameterizedType.getOwnerType).map(owner => resolveType(owner, typeEnv)).orNull
        val resolvedTypeArguments =
          parameterizedType.getActualTypeArguments.map(typeArg => resolveType(typeArg, typeEnv))
        SimpleParameterizedType(resolvedRawType, resolvedTypeArguments, resolvedOwnerType)

      case rawClass: Class[_] =>
        rawClass

      case otherType =>
        otherType
    }

  /** Retrieves the best available TypeEnv for a specific object instance. */
  private def envFor(instance: AnyRef): TypeEnv = {
    val storedEnv = Option(envByObj.get(instance)).getOrElse(Map.empty)
    val classDerivedEnv = computeEnvFromConcreteClass(instance.getClass)
    classDerivedEnv ++ storedEnv
  }

  /**
    * Extends an existing TypeEnv with (rawClass type params -> resolved type args).
    * Used when instantiating parameterized types so child object graphs can be reasoned about.
    */
  private def envForParameterizedInstance(
      rawClass: Class[_],
      typeArguments: scala.Array[Type],
      parentTypeEnv: TypeEnv
  ): TypeEnv = {
    val rawTypeVariables = rawClass.getTypeParameters
    val resolvedTypeArguments = typeArguments.map(typeArg => resolveType(typeArg, parentTypeEnv))
    val rawTypeVarBindings = rawTypeVariables
      .zipAll(resolvedTypeArguments, null, null)
      .collect {
        case (typeVar, typeArg) if typeVar != null && typeArg != null => typeVar -> typeArg
      }
      .toMap
    parentTypeEnv ++ rawTypeVarBindings
  }

  private def typeToClass(typ: Type): Option[Class[_]] =
    typ match {
      case rawClass: Class[_]                   => Some(rawClass)
      case parameterizedType: ParameterizedType => typeToClass(parameterizedType.getRawType)
      case _                                    => None
    }

  private def elementTypeOfResolved(resolvedType: Type): Option[Type] =
    resolvedType match {
      case parameterizedType: ParameterizedType =>
        parameterizedType.getActualTypeArguments.headOption
      case arrayClass: Class[_] if arrayClass.isArray =>
        Some(arrayClass.getComponentType)
      case _ =>
        None
    }

  // ------------------------------------------------------------
  // Best-effort init (generic-aware)
  // ------------------------------------------------------------

  /**
    * Best-effort initialization for @JsonProperty fields:
    *  - If @JsonProperty(required = true) or defaultValue is provided, tries to initialize when null.
    *  - Also ensures required collections are non-empty (adds an element when element type can be inferred).
    *
    * This is intentionally heuristic: the goal is to create a "usable enough" object graph for codegen
    * without knowing real business semantics.
    */
  private def bestEffortFillJsonPropertyDefaults(
      rootDescriptor: AnyRef,
      recursionDepthLimit: Int
  ): Unit = {
    val visitedIdentityHashes = mutable.Set.empty[Int]

    def fillRecursively(currentObject: AnyRef, remainingDepth: Int): Unit = {
      if (currentObject == null || remainingDepth < 0) return
      val objectId = System.identityHashCode(currentObject)
      if (visitedIdentityHashes.contains(objectId)) return
      visitedIdentityHashes += objectId

      val currentTypeEnv = envFor(currentObject)

      walkHierarchy(currentObject.getClass) { declaringClassInHierarchy =>
        declaringClassInHierarchy.getDeclaredFields.foreach { declaredField =>
          if (!shouldSkipField(declaredField)) {
            val jsonPropertyOpt =
              jsonPropertyForFieldOrAccessors(declaringClassInHierarchy, declaredField)
            jsonPropertyOpt.foreach { jsonPropertyAnn =>
              declaredField.setAccessible(true)

              val currentFieldValue = Try(declaredField.get(currentObject)).toOption.orNull
              val defaultValueText = Option(jsonPropertyAnn.defaultValue()).getOrElse("").trim
              val isRequired = jsonPropertyAnn.required()

              val resolvedFieldType = resolveType(declaredField.getGenericType, currentTypeEnv)
              val needsInitialization =
                (currentFieldValue == null) && (isRequired || defaultValueText.nonEmpty)

              val ensuredValue: AnyRef =
                if (needsInitialization) {
                  val defaultValue = defaultValueForResolvedType(
                    targetType = resolvedFieldType,
                    defaultValueText = defaultValueText,
                    remainingDepth = remainingDepth,
                    typeEnvAtParent = currentTypeEnv
                  )
                  if (defaultValue != null) {
                    trySet(currentObject, declaringClassInHierarchy, declaredField, defaultValue)
                    defaultValue
                  } else currentFieldValue
                } else currentFieldValue

              val updatedValue =
                ensureNonEmptyIfRequired(
                  owningInstance = currentObject,
                  declaringClass = declaringClassInHierarchy,
                  field = declaredField,
                  currentFieldValue = ensuredValue,
                  jsonPropertyAnn = jsonPropertyAnn,
                  resolvedFieldType = resolvedFieldType,
                  typeEnvAtField = currentTypeEnv,
                  remainingDepth = remainingDepth
                )

              recurseIntoValue(updatedValue, remainingDepth - 1, fillRecursively)
            }
          }
        }
      }
    }

    fillRecursively(rootDescriptor, recursionDepthLimit)
  }

  private def ensureNonEmptyIfRequired(
      owningInstance: AnyRef,
      declaringClass: Class[_],
      field: Field,
      currentFieldValue: AnyRef,
      jsonPropertyAnn: JsonProperty,
      resolvedFieldType: Type,
      typeEnvAtField: TypeEnv,
      remainingDepth: Int
  ): AnyRef = {
    if (!jsonPropertyAnn.required() || remainingDepth <= 0) return currentFieldValue

    // If required and null, try to initialize collection containers too
    val ensuredNonNullValue: AnyRef =
      if (currentFieldValue != null) currentFieldValue
      else {
        val rawFieldClass = typeToClass(resolvedFieldType).getOrElse(field.getType)
        val defaultValue = defaultValueForResolvedType(
          targetType = rawFieldClass,
          defaultValueText = "",
          remainingDepth = remainingDepth,
          typeEnvAtParent = typeEnvAtField
        )
        if (defaultValue != null) trySet(owningInstance, declaringClass, field, defaultValue)
        defaultValue
      }

    if (ensuredNonNullValue == null) return null

    val runtimeValueClass = ensuredNonNullValue.getClass
    val elementTypeOpt =
      elementTypeOfResolved(resolvedFieldType).map(et => resolveType(et, typeEnvAtField))

    def makeElementValue(): AnyRef = {
      val elementType = elementTypeOpt.getOrElse(classOf[String])
      defaultValueForResolvedType(
        targetType = elementType,
        defaultValueText = "",
        remainingDepth = remainingDepth - 1,
        typeEnvAtParent = typeEnvAtField
      )
    }

    if (isJavaList(runtimeValueClass)) {
      val javaList = ensuredNonNullValue.asInstanceOf[util.List[AnyRef]]
      if (javaList.isEmpty) {
        val elementValue = makeElementValue()
        if (elementValue != null) javaList.add(elementValue)
      }
    } else if (isScalaIterable(runtimeValueClass)) {
      val scalaIterable = ensuredNonNullValue.asInstanceOf[scala.collection.Iterable[Any]]
      if (scalaIterable.isEmpty) {
        val elementValue = makeElementValue()
        if (elementValue != null)
          trySet(owningInstance, declaringClass, field, List(elementValue).asInstanceOf[AnyRef])
      }
    } else if (runtimeValueClass.isArray && runtimeValueClass.getComponentType == classOf[String]) {
      val stringArray = ensuredNonNullValue.asInstanceOf[scala.Array[String]]
      if (stringArray.isEmpty)
        trySet(owningInstance, declaringClass, field, scala.Array("x").asInstanceOf[AnyRef])
    }

    Try(field.get(owningInstance)).toOption.orNull
  }

  private def defaultValueForResolvedType(
      targetType: Type,
      defaultValueText: String,
      remainingDepth: Int,
      typeEnvAtParent: TypeEnv
  ): AnyRef = {
    val trimmedDefaultValueText = Option(defaultValueText).getOrElse("").trim
    val resolvedTargetType = resolveType(targetType, typeEnvAtParent)

    resolvedTargetType match {
      case rawClass: Class[_] =>
        if (rawClass == classOf[String]) {
          if (trimmedDefaultValueText.nonEmpty) trimmedDefaultValueText else "x"
        } else if (rawClass == java.lang.Boolean.TYPE || rawClass == classOf[java.lang.Boolean]) {
          val booleanValue = trimmedDefaultValueText.toLowerCase match {
            case "true"  => true
            case "false" => false
            case _       => false
          }
          java.lang.Boolean.valueOf(booleanValue)
        } else if (rawClass == java.lang.Integer.TYPE || rawClass == classOf[java.lang.Integer]) {
          java.lang.Integer.valueOf(Try(trimmedDefaultValueText.toInt).getOrElse(1))
        } else if (rawClass == java.lang.Long.TYPE || rawClass == classOf[java.lang.Long]) {
          java.lang.Long.valueOf(Try(trimmedDefaultValueText.toLong).getOrElse(1L))
        } else if (rawClass == java.lang.Double.TYPE || rawClass == classOf[java.lang.Double]) {
          java.lang.Double.valueOf(Try(trimmedDefaultValueText.toDouble).getOrElse(1.0d))
        } else if (rawClass == java.lang.Float.TYPE || rawClass == classOf[java.lang.Float]) {
          java.lang.Float.valueOf(Try(trimmedDefaultValueText.toFloat).getOrElse(1.0f))
        } else if (rawClass.isEnum) {
          chooseEnumConstant(rawClass, trimmedDefaultValueText)
        } else if (isJavaList(rawClass)) {
          new util.ArrayList[AnyRef]()
        } else if (isScalaIterable(rawClass)) {
          List.empty[Any]
        } else if (rawClass.isArray && rawClass.getComponentType == classOf[String]) {
          scala.Array.empty[String]
        } else if (classOf[scala.Option[_]].isAssignableFrom(rawClass)) {
          None
        } else if (
          !rawClass.isInterface && !Modifier.isAbstract(rawClass.getModifiers) && remainingDepth > 0
        ) {
          instantiateBestEffort(rawClass).orNull
        } else null

      case parameterizedType: ParameterizedType =>
        val rawClass = typeToClass(parameterizedType.getRawType).orNull
        if (rawClass == null) return null

        if (rawClass.isEnum) {
          chooseEnumConstant(rawClass, trimmedDefaultValueText)
        } else if (isJavaList(rawClass)) {
          new util.ArrayList[AnyRef]()
        } else if (isScalaIterable(rawClass)) {
          List.empty[Any]
        } else if (classOf[scala.Option[_]].isAssignableFrom(rawClass)) {
          None
        } else if (
          !rawClass.isInterface && !Modifier.isAbstract(rawClass.getModifiers) && remainingDepth > 0
        ) {
          val instanceOpt = instantiateBestEffort(rawClass)
          instanceOpt.foreach { newInstance =>
            val newInstanceTypeEnv =
              envForParameterizedInstance(
                rawClass,
                parameterizedType.getActualTypeArguments,
                typeEnvAtParent
              )
            envByObj.put(newInstance, newInstanceTypeEnv)
          }
          instanceOpt.orNull
        } else null

      case _ =>
        null
    }
  }

  /**
    * Attempts to set a value into a field through multiple strategies:
    *  1) Direct reflective field set
    *  2) Scala setter: fieldName_$eq
    *  3) JavaBean setter: setFieldName
    */
  private def trySet(
      owningInstance: AnyRef,
      declaringClass: Class[_],
      field: Field,
      newValue: AnyRef
  ): Unit = {
    // 1) Try direct field set
    val didSetViaField = Try {
      field.setAccessible(true); field.set(owningInstance, newValue)
    }.isSuccess
    if (didSetViaField) return

    // 2) Try Scala setter: name_$eq
    val scalaSetterName = field.getName + "_$eq"
    val didInvokeScalaSetter = Try {
      val matchingMethodOpt =
        declaringClass.getDeclaredMethods.find(m =>
          m.getName == scalaSetterName && m.getParameterCount == 1
        )
      matchingMethodOpt.foreach { setterMethod =>
        setterMethod.setAccessible(true)
        setterMethod.invoke(owningInstance, newValue.asInstanceOf[Object])
      }
      matchingMethodOpt.isDefined
    }.getOrElse(false)
    if (didInvokeScalaSetter) return

    // 3) Try JavaBean setter: setX
    val javaBeanSetterName = "set" + upperFirst(field.getName)
    Try {
      val matchingMethodOpt =
        declaringClass.getDeclaredMethods.find(m =>
          m.getName == javaBeanSetterName && m.getParameterCount == 1
        )
      matchingMethodOpt.foreach { setterMethod =>
        setterMethod.setAccessible(true)
        setterMethod.invoke(owningInstance, newValue.asInstanceOf[Object])
      }
    }
    ()
  }

  // ------------------------------------------------------------
  // Raw Invalid String Detection (generic-aware)
  // ------------------------------------------------------------

  /**
    * Replaces string values in @JsonProperty fields (and string containers) with the rawInvalidText marker.
    *
    * Returns which members were changed and which ones could not be changed.
    */
  private def rawInvalidTextJsonPropertyStringsDeep(
      rootDescriptor: AnyRef,
      rawInvalidTextMarker: String,
      recursionDepthLimit: Int
  ): RawInvalidTextResult = {
    val changedMembers = mutable.ArrayBuffer.empty[String]
    val failedMembers = mutable.ArrayBuffer.empty[String]
    val visitedIdentityHashes = mutable.Set.empty[Int]

    def rawInvalidTextRecursively(currentObject: AnyRef, remainingDepth: Int): Unit = {
      if (currentObject == null || remainingDepth < 0) return
      val objectId = System.identityHashCode(currentObject)
      if (visitedIdentityHashes.contains(objectId)) return
      visitedIdentityHashes += objectId

      val currentTypeEnv = envFor(currentObject)

      walkHierarchy(currentObject.getClass) { declaringClassInHierarchy =>
        declaringClassInHierarchy.getDeclaredFields.foreach { declaredField =>
          if (!shouldSkipField(declaredField)) {
            val jsonPropertyOpt =
              jsonPropertyForFieldOrAccessors(declaringClassInHierarchy, declaredField)
            jsonPropertyOpt.foreach { jsonPropertyAnn =>
              declaredField.setAccessible(true)
              val jsonPropertyName =
                effectiveJsonPropName(jsonPropertyAnn, fallback = declaredField.getName)

              val resolvedFieldType = resolveType(declaredField.getGenericType, currentTypeEnv)
              val rawFieldClass = typeToClass(resolvedFieldType).getOrElse(declaredField.getType)
              val currentFieldValue = Try(declaredField.get(currentObject)).toOption.orNull

              if (rawFieldClass == classOf[String]) {
                val didInjected = Try {
                  trySet(
                    currentObject,
                    declaringClassInHierarchy,
                    declaredField,
                    rawInvalidTextMarker
                  )
                }.isSuccess
                if (didInjected)
                  changedMembers += s"""${declaringClassInHierarchy.getSimpleName}.${declaredField.getName}(@JsonProperty("$jsonPropertyName"))"""
                else
                  failedMembers += s"${declaringClassInHierarchy.getSimpleName}.${declaredField.getName}"

              } else if (isJavaList(rawFieldClass)) {
                val javaListValue =
                  if (currentFieldValue != null) currentFieldValue.asInstanceOf[util.List[AnyRef]]
                  else {
                    val newList = new util.ArrayList[AnyRef]()
                    Try(trySet(currentObject, declaringClassInHierarchy, declaredField, newList))
                    newList
                  }

                val isElementTypeString = elementTypeOfResolved(resolvedFieldType)
                  .map(et => resolveType(et, currentTypeEnv))
                  .flatMap(typeToClass)
                  .contains(classOf[String])

                if (isElementTypeString) {
                  Try { javaListValue.clear(); javaListValue.add(rawInvalidTextMarker) }
                  changedMembers += s"""${declaringClassInHierarchy.getSimpleName}.${declaredField.getName}[0](@JsonProperty("$jsonPropertyName"))"""
                } else {
                  javaListValue.asScala.foreach(elementObj =>
                    rawInvalidTextRecursively(elementObj, remainingDepth - 1)
                  )
                }

              } else if (isScalaIterable(rawFieldClass)) {
                val isElementTypeString = elementTypeOfResolved(resolvedFieldType)
                  .map(et => resolveType(et, currentTypeEnv))
                  .flatMap(typeToClass)
                  .contains(classOf[String])

                if (isElementTypeString) {
                  val didSetList =
                    Try(
                      trySet(
                        currentObject,
                        declaringClassInHierarchy,
                        declaredField,
                        List(rawInvalidTextMarker).asInstanceOf[AnyRef]
                      )
                    ).isSuccess
                  if (didSetList)
                    changedMembers += s"""${declaringClassInHierarchy.getSimpleName}.${declaredField.getName}[0](@JsonProperty("$jsonPropertyName"))"""
                  else
                    failedMembers += s"${declaringClassInHierarchy.getSimpleName}.${declaredField.getName}"
                } else {
                  recurseIntoValue(currentFieldValue, remainingDepth - 1, rawInvalidTextRecursively)
                }

              } else if (
                rawFieldClass.isArray && rawFieldClass.getComponentType == classOf[String]
              ) {
                val didInjectedArray =
                  Try(
                    trySet(
                      currentObject,
                      declaringClassInHierarchy,
                      declaredField,
                      scala.Array(rawInvalidTextMarker).asInstanceOf[AnyRef]
                    )
                  ).isSuccess
                if (didInjectedArray)
                  changedMembers += s"""${declaringClassInHierarchy.getSimpleName}.${declaredField.getName}[0](@JsonProperty("$jsonPropertyName"))"""
                else
                  failedMembers += s"${declaringClassInHierarchy.getSimpleName}.${declaredField.getName}"

              } else {
                recurseIntoValue(currentFieldValue, remainingDepth - 1, rawInvalidTextRecursively)
              }
            }
          }
        }
      }
    }

    rawInvalidTextRecursively(rootDescriptor, recursionDepthLimit)
    RawInvalidTextResult(changedMembers.distinct.toSeq, failedMembers.distinct.toSeq)
  }

  // ------------------------------------------------------------
  // Reflection utilities
  // ------------------------------------------------------------

  /** Walks the class hierarchy from `startingClass` up to (excluding) java.lang.Object. */
  private def walkHierarchy(startingClass: Class[_])(visitFn: Class[_] => Unit): Unit = {
    var currentClass: Class[_] = startingClass
    while (currentClass != null && currentClass != classOf[Object]) {
      visitFn(currentClass)
      currentClass = currentClass.getSuperclass
    }
  }

  /** Filters out synthetic, compiler-generated, and static fields (things we should not involve with). */
  private def shouldSkipField(field: Field): Boolean = {
    field.isSynthetic || field.getName.contains("$") || Modifier.isStatic(field.getModifiers)
  }

  private def upperFirst(text: String): String =
    if (text.isEmpty) text else s"${text.charAt(0).toUpper}${text.substring(1)}"

  /**
    * Finds a @JsonProperty annotation either on:
    *  - The field itself, or
    *  - A getter/setter method that corresponds to the field name (Scala/Java styles).
    */
  private def jsonPropertyForFieldOrAccessors(
      declaringClass: Class[_],
      field: Field
  ): Option[JsonProperty] = {
    Option(field.getAnnotation(classOf[JsonProperty])).orElse {
      val fieldName = field.getName
      val getterMethodNames =
        Seq(fieldName, "get" + upperFirst(fieldName), "is" + upperFirst(fieldName))
      val setterMethodNames = Seq(fieldName + "_$eq", "set" + upperFirst(fieldName))

      val declaredMethods = declaringClass.getDeclaredMethods
      def annotationOn(methodName: String, expectedParamCount: Int): Option[JsonProperty] =
        declaredMethods
          .find(m =>
            m.getName == methodName && !m.isSynthetic && m.getParameterCount == expectedParamCount
          )
          .flatMap(m => Option(m.getAnnotation(classOf[JsonProperty])))

      getterMethodNames.iterator
        .map(candidateName => annotationOn(candidateName, 0))
        .find(_.nonEmpty)
        .flatten
        .orElse(
          setterMethodNames.iterator
            .map(candidateName => annotationOn(candidateName, 1))
            .find(_.nonEmpty)
            .flatten
        )
    }
  }

  private def effectiveJsonPropName(jsonPropertyAnn: JsonProperty, fallback: String): String = {
    val explicitName = Option(jsonPropertyAnn.value()).getOrElse("").trim
    if (explicitName.nonEmpty) explicitName else fallback
  }

  private def isJavaList(clazz: Class[_]): Boolean =
    classOf[util.List[_]].isAssignableFrom(clazz)

  private def isScalaIterable(clazz: Class[_]): Boolean =
    classOf[scala.collection.Iterable[_]].isAssignableFrom(clazz) ||
      classOf[scala.collection.Seq[_]].isAssignableFrom(clazz)

  private def chooseEnumConstant(enumClass: Class[_], desiredValue: String): AnyRef = {
    val enumConstants = enumClass.getEnumConstants.asInstanceOf[scala.Array[AnyRef]]
    if (enumConstants == null || enumConstants.isEmpty) return null

    val desiredLower = Option(desiredValue).getOrElse("").trim.toLowerCase
    if (desiredLower.isEmpty) return enumConstants.head

    def getNameViaReflection(enumValue: AnyRef): Option[String] =
      Try {
        val getNameMethod = enumValue.getClass.getMethod("getName")
        getNameMethod.setAccessible(true)
        getNameMethod.invoke(enumValue).toString
      }.toOption

    enumConstants
      .find { constant =>
        val enumName = Try(constant.asInstanceOf[Enum[_]].name()).toOption.getOrElse("")
        val stringRepr = constant.toString.toLowerCase
        val enumNameLower = enumName.toLowerCase
        val reflectedNameLower = getNameViaReflection(constant).getOrElse("").toLowerCase
        stringRepr == desiredLower || enumNameLower == desiredLower || reflectedNameLower == desiredLower
      }
      .getOrElse(enumConstants.head)
  }

  /**
    * Best-effort instantiation for arbitrary classes:
    *  - Scala object (MODULE$), else
    *  - No-arg constructor.
    */
  private def instantiateBestEffort(clazz: Class[_]): Option[AnyRef] = {
    val scalaModuleInstanceOpt = Try(clazz.getField("MODULE$")).toOption
      .orElse(Try(clazz.getDeclaredField("MODULE$")).toOption)
      .flatMap { moduleField =>
        Try { moduleField.setAccessible(true); moduleField.get(null).asInstanceOf[AnyRef] }.toOption
      }

    scalaModuleInstanceOpt.orElse {
      Try {
        val noArgConstructor = clazz.getDeclaredConstructor()
        noArgConstructor.setAccessible(true)
        noArgConstructor.newInstance().asInstanceOf[AnyRef]
      }.toOption
    }
  }

  /**
    * Recurses into:
    *  - Java Lists
    *  - Scala Iterables
    *  - Arrays
    *  - Arbitrary non-leaf objects (excludes primitives, boxed primitives, String, enums, and core java/scala packages)
    */
  private def recurseIntoValue(
      value: AnyRef,
      remainingDepth: Int,
      visitFn: (AnyRef, Int) => Unit
  ): Unit = {
    if (value == null || remainingDepth < 0) return

    value match {
      case javaList: util.List[_] =>
        javaList.asScala.foreach {
          case elementRef: AnyRef => visitFn(elementRef, remainingDepth)
          case _                  => ()
        }

      case scalaIterable: scala.collection.Iterable[_] =>
        scalaIterable.foreach {
          case elementRef: AnyRef => visitFn(elementRef, remainingDepth)
          case _                  => ()
        }

      case arrayValue: scala.Array[_] =>
        arrayValue.foreach {
          case elementRef: AnyRef => visitFn(elementRef, remainingDepth)
          case _                  => ()
        }

      case otherValue =>
        val runtimeClass = otherValue.getClass
        val isLeafValue =
          runtimeClass.isPrimitive ||
            runtimeClass == classOf[String] ||
            classOf[java.lang.Number].isAssignableFrom(runtimeClass) ||
            runtimeClass == classOf[java.lang.Boolean] ||
            runtimeClass.isEnum ||
            runtimeClass.getName.startsWith("java.") ||
            runtimeClass.getName.startsWith("javax.") ||
            runtimeClass.getName.startsWith("scala.")

        if (!isLeafValue) visitFn(otherValue, remainingDepth)
    }
  }
}
