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
  private final case class JsonFieldSite(
      owner: AnyRef,
      declaringClass: Class[_],
      field: Field,
      jsonProp: JsonProperty,
      typeEnv: TypeEnv,
      resolvedType: Type,
      currentValue: AnyRef,
      remainingDepth: Int
  )

  private def walkJsonPropertyFields(
      rootDescriptor: AnyRef,
      recursionDepthLimit: Int
  )(visitField: (JsonFieldSite, (AnyRef, Int) => Unit) => Unit): Unit = {
    val visitedIdentityHashes = mutable.Set.empty[Int]

    def visitObject(currentObject: AnyRef, remainingDepth: Int): Unit = {
      if (currentObject == null || remainingDepth < 0) return
      val objectId = System.identityHashCode(currentObject)
      if (!visitedIdentityHashes.add(objectId)) return

      val currentTypeEnv = envFor(currentObject)
      walkHierarchy(currentObject.getClass) { declaringClass =>
        declaringClass.getDeclaredFields.foreach { field =>
          if (!shouldSkipField(field)) {
            jsonPropertyForFieldOrAccessors(declaringClass, field).foreach { jsonProp =>
              field.setAccessible(true)
              val site = JsonFieldSite(
                owner = currentObject,
                declaringClass = declaringClass,
                field = field,
                jsonProp = jsonProp,
                typeEnv = currentTypeEnv,
                resolvedType = resolveType(field.getGenericType, currentTypeEnv),
                currentValue = Try(field.get(currentObject)).toOption.orNull,
                remainingDepth = remainingDepth
              )
              visitField(site, visitObject)
            }
          }
        }
      }
    }

    visitObject(rootDescriptor, recursionDepthLimit)
  }

  private def bestEffortFillJsonPropertyDefaults(
      rootDescriptor: AnyRef,
      recursionDepthLimit: Int
  ): Unit = {
    walkJsonPropertyFields(rootDescriptor, recursionDepthLimit) { (site, recurse) =>
      val defaultValueText = Option(site.jsonProp.defaultValue()).getOrElse("").trim
      val needsInitialization =
        (site.currentValue == null) && (site.jsonProp.required() || defaultValueText.nonEmpty)

      val ensuredValue: AnyRef =
        if (needsInitialization) {
          val defaultValue = defaultValueForResolvedType(
            targetType = site.resolvedType,
            defaultValueText = defaultValueText,
            remainingDepth = site.remainingDepth,
            typeEnvAtParent = site.typeEnv
          )
          if (defaultValue != null) {
            trySet(site.owner, site.declaringClass, site.field, defaultValue)
            defaultValue
          } else site.currentValue
        } else site.currentValue

      val updatedValue = ensureNonEmptyIfRequired(site, ensuredValue)
      recurseIntoValue(updatedValue, site.remainingDepth - 1, recurse)
    }
  }

  private def ensureNonEmptyIfRequired(site: JsonFieldSite, currentFieldValue: AnyRef): AnyRef = {
    if (!site.jsonProp.required() || site.remainingDepth <= 0) return currentFieldValue

    // If required and null, try to initialize collection containers too
    val ensuredNonNullValue: AnyRef =
      if (currentFieldValue != null) currentFieldValue
      else {
        val rawFieldClass = typeToClass(site.resolvedType).getOrElse(site.field.getType)
        val defaultValue =
          defaultValueForResolvedType(rawFieldClass, "", site.remainingDepth, site.typeEnv)
        if (defaultValue != null) trySet(site.owner, site.declaringClass, site.field, defaultValue)
        defaultValue
      }

    if (ensuredNonNullValue == null) return null

    val runtimeValueClass = ensuredNonNullValue.getClass
    val elementTypeOpt =
      elementTypeOfResolved(site.resolvedType).map(et => resolveType(et, site.typeEnv))

    def makeElementValue(): AnyRef =
      defaultValueForResolvedType(
        targetType = elementTypeOpt.getOrElse(classOf[String]),
        defaultValueText = "",
        remainingDepth = site.remainingDepth - 1,
        typeEnvAtParent = site.typeEnv
      )

    if (isJavaList(runtimeValueClass)) {
      val javaList = ensuredNonNullValue.asInstanceOf[util.List[AnyRef]]
      if (javaList.isEmpty) {
        Option(makeElementValue()).foreach(javaList.add)
      }
    } else if (isScalaIterable(runtimeValueClass)) {
      val scalaIterable = ensuredNonNullValue.asInstanceOf[scala.collection.Iterable[Any]]
      if (scalaIterable.isEmpty) {
        Option(makeElementValue()).foreach(e =>
          trySet(site.owner, site.declaringClass, site.field, List(e).asInstanceOf[AnyRef])
        )
      }
    } else if (runtimeValueClass.isArray && runtimeValueClass.getComponentType == classOf[String]) {
      val stringArray = ensuredNonNullValue.asInstanceOf[scala.Array[String]]
      if (stringArray.isEmpty)
        trySet(site.owner, site.declaringClass, site.field, scala.Array("x").asInstanceOf[AnyRef])
    }

    Try(site.field.get(site.owner)).toOption.orNull
  }

  private def defaultValueForClass(
      rawClass: Class[_],
      trimmedDefaultValueText: String,
      remainingDepth: Int,
      parameterizedOpt: Option[ParameterizedType],
      typeEnvAtParent: TypeEnv
  ): AnyRef = {
    if (rawClass.isEnum) {
      chooseEnumConstant(rawClass, trimmedDefaultValueText)
    } else if (isJavaList(rawClass)) {
      new util.ArrayList[AnyRef]()
    } else if (isScalaIterable(rawClass)) {
      List.empty[Any]
    } else if (
      parameterizedOpt.isEmpty && rawClass.isArray && rawClass.getComponentType == classOf[String]
    ) {
      scala.Array.empty[String]
    } else if (classOf[scala.Option[_]].isAssignableFrom(rawClass)) {
      None
    } else if (
      !rawClass.isInterface && !Modifier.isAbstract(rawClass.getModifiers) && remainingDepth > 0
    ) {
      val instanceOpt = instantiateBestEffort(rawClass)
      for {
        pt <- parameterizedOpt
        newInstance <- instanceOpt
      } {
        val newInstanceTypeEnv =
          envForParameterizedInstance(rawClass, pt.getActualTypeArguments, typeEnvAtParent)
        envByObj.put(newInstance, newInstanceTypeEnv)
      }
      instanceOpt.orNull
    } else null
  }

  private def defaultValueForResolvedType(
      targetType: Type,
      defaultValueText: String,
      remainingDepth: Int,
      typeEnvAtParent: TypeEnv
  ): AnyRef = {
    val trimmedDefaultValueText = Option(defaultValueText).getOrElse("").trim
    resolveType(targetType, typeEnvAtParent) match {
      case rawClass: Class[_] =>
        if (rawClass == classOf[String]) {
          if (trimmedDefaultValueText.nonEmpty) trimmedDefaultValueText else "x"
        } else if (rawClass == java.lang.Boolean.TYPE || rawClass == classOf[java.lang.Boolean]) {
          java.lang.Boolean.valueOf(trimmedDefaultValueText.toLowerCase == "true")
        } else if (rawClass == java.lang.Integer.TYPE || rawClass == classOf[java.lang.Integer]) {
          java.lang.Integer.valueOf(Try(trimmedDefaultValueText.toInt).getOrElse(1))
        } else if (rawClass == java.lang.Long.TYPE || rawClass == classOf[java.lang.Long]) {
          java.lang.Long.valueOf(Try(trimmedDefaultValueText.toLong).getOrElse(1L))
        } else if (rawClass == java.lang.Double.TYPE || rawClass == classOf[java.lang.Double]) {
          java.lang.Double.valueOf(Try(trimmedDefaultValueText.toDouble).getOrElse(1.0d))
        } else if (rawClass == java.lang.Float.TYPE || rawClass == classOf[java.lang.Float]) {
          java.lang.Float.valueOf(Try(trimmedDefaultValueText.toFloat).getOrElse(1.0f))
        } else {
          defaultValueForClass(
            rawClass,
            trimmedDefaultValueText,
            remainingDepth,
            None,
            typeEnvAtParent
          )
        }

      case parameterizedType: ParameterizedType =>
        typeToClass(parameterizedType.getRawType) match {
          case Some(rawClass) =>
            defaultValueForClass(
              rawClass,
              trimmedDefaultValueText,
              remainingDepth,
              Some(parameterizedType),
              typeEnvAtParent
            )
          case None => null
        }

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

    def hasStringElementType(site: JsonFieldSite): Boolean =
      elementTypeOfResolved(site.resolvedType)
        .map(et => resolveType(et, site.typeEnv))
        .flatMap(typeToClass)
        .contains(classOf[String])

    def injectFieldValue(site: JsonFieldSite, value: AnyRef, elementSuffix: String = ""): Unit = {
      val jsonPropertyName = effectiveJsonPropName(site.jsonProp, fallback = site.field.getName)
      val prefix = s"${site.declaringClass.getSimpleName}.${site.field.getName}"
      if (Try(trySet(site.owner, site.declaringClass, site.field, value)).isSuccess)
        changedMembers += s"""$prefix$elementSuffix(@JsonProperty("$jsonPropertyName"))"""
      else
        failedMembers += prefix
    }

    walkJsonPropertyFields(rootDescriptor, recursionDepthLimit) { (site, recurse) =>
      val rawFieldClass = typeToClass(site.resolvedType).getOrElse(site.field.getType)

      if (rawFieldClass == classOf[String]) {
        injectFieldValue(site, rawInvalidTextMarker)
      } else if (isJavaList(rawFieldClass)) {
        val javaListValue =
          if (site.currentValue != null) site.currentValue.asInstanceOf[util.List[AnyRef]]
          else {
            val newList = new util.ArrayList[AnyRef]()
            Try(trySet(site.owner, site.declaringClass, site.field, newList))
            newList
          }

        if (hasStringElementType(site)) {
          Try { javaListValue.clear(); javaListValue.add(rawInvalidTextMarker) }
          val jsonPropertyName = effectiveJsonPropName(site.jsonProp, fallback = site.field.getName)
          changedMembers += s"""${site.declaringClass.getSimpleName}.${site.field.getName}[0](@JsonProperty("$jsonPropertyName"))"""
        } else {
          javaListValue.asScala.foreach(elementObj => recurse(elementObj, site.remainingDepth - 1))
        }
      } else if (isScalaIterable(rawFieldClass)) {
        if (hasStringElementType(site)) {
          injectFieldValue(site, List(rawInvalidTextMarker).asInstanceOf[AnyRef], "[0]")
        } else {
          recurseIntoValue(site.currentValue, site.remainingDepth - 1, recurse)
        }
      } else if (rawFieldClass.isArray && rawFieldClass.getComponentType == classOf[String]) {
        injectFieldValue(site, scala.Array(rawInvalidTextMarker).asInstanceOf[AnyRef], "[0]")
      } else {
        recurseIntoValue(site.currentValue, site.remainingDepth - 1, recurse)
      }
    }

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

    val elements: Option[Iterator[_]] = value match {
      case javaList: util.List[_]                      => Some(javaList.asScala.iterator)
      case scalaIterable: scala.collection.Iterable[_] => Some(scalaIterable.iterator)
      case arrayValue: scala.Array[_]                  => Some(arrayValue.iterator)
      case _                                           => None
    }
    elements match {
      case Some(it) =>
        it.foreach {
          case elementRef: AnyRef => visitFn(elementRef, remainingDepth)
          case _                  => ()
        }
      case None =>
        val runtimeClass = value.getClass
        val isLeafValue =
          runtimeClass.isPrimitive ||
            runtimeClass == classOf[String] ||
            classOf[java.lang.Number].isAssignableFrom(runtimeClass) ||
            runtimeClass == classOf[java.lang.Boolean] ||
            runtimeClass.isEnum ||
            runtimeClass.getName.startsWith("java.") ||
            runtimeClass.getName.startsWith("javax.") ||
            runtimeClass.getName.startsWith("scala.")

        if (!isLeafValue) visitFn(value, remainingDepth)
    }
  }
}
