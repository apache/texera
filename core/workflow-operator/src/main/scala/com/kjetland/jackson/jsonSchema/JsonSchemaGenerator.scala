package com.kjetland.jackson.jsonSchema

import java.lang.annotation.Annotation
import java.util
import java.util.function.Supplier
import java.util.{Optional, List => JList}
import com.fasterxml.jackson.annotation.{
  JsonProperty,
  JsonPropertyDescription,
  JsonSubTypes,
  JsonTypeInfo,
  JsonTypeName
}
import com.fasterxml.jackson.core.JsonParser.NumberType
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.introspect.{AnnotatedClass, AnnotatedClassResolver}
import com.fasterxml.jackson.databind.jsonFormatVisitors._
import com.fasterxml.jackson.databind.jsontype.impl.MinimalClassNameIdResolver
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode}
import com.fasterxml.jackson.databind.util.ClassUtil
import com.kjetland.jackson.jsonSchema.annotations._
import io.github.classgraph.{ClassGraph, ScanResult}

import javax.validation.constraints._
import javax.validation.groups.Default
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsScala}

object JsonSchemaGenerator {}

object JsonSchemaConfig {

  val vanillaJsonSchemaDraft4: JsonSchemaConfig = JsonSchemaConfig(
    autoGenerateTitleForProperties = false,
    defaultArrayFormat = None,
    useOneOfForOption = false,
    useOneOfForNullables = false,
    useNullableForOption = false,
    useNullableForNullables = false,
    usePropertyOrdering = false,
    hidePolymorphismTypeProperty = false,
    disableWarnings = false,
    useMinLengthForNotNull = false,
    useTypeIdForDefinitionName = false,
    customType2FormatMapping = Map(),
    useMultipleEditorSelectViaProperty = false,
    uniqueItemClasses = Set(),
    classTypeReMapping = Map(),
    jsonSuppliers = Map()
  )

  /**
    * Use this configuration if using the JsonSchema to generate HTML5 GUI, eg. by using https://github.com/jdorn/json-editor
    *
    * autoGenerateTitleForProperties - If property is named "someName", we will add {"title": "Some Name"}
    * defaultArrayFormat - this will result in a better gui than te default one.
    */
  val html5EnabledSchema: JsonSchemaConfig = JsonSchemaConfig(
    autoGenerateTitleForProperties = true,
    defaultArrayFormat = Some("table"),
    useOneOfForOption = true,
    useOneOfForNullables = false,
    useNullableForOption = false,
    useNullableForNullables = false,
    usePropertyOrdering = true,
    hidePolymorphismTypeProperty = true,
    disableWarnings = false,
    useMinLengthForNotNull = true,
    useTypeIdForDefinitionName = false,
    customType2FormatMapping = Map[String, String](
      // Java7 dates
      "java.time.LocalDateTime" -> "datetime-local",
      "java.time.OffsetDateTime" -> "datetime",
      "java.time.LocalDate" -> "date",
      // Joda-dates
      "org.joda.time.LocalDate" -> "date"
    ),
    useMultipleEditorSelectViaProperty = true,
    uniqueItemClasses = Set(
      classOf[scala.collection.immutable.Set[_]],
      classOf[scala.collection.mutable.Set[_]],
      classOf[java.util.Set[_]]
    ),
    classTypeReMapping = Map(),
    jsonSuppliers = Map()
  )

  /**
    * This configuration is exactly like the vanilla JSON schema generator, except that "nullables" have been turned on:
    * `useOneOfForOption` and `useOneForNullables` have both been set to `true`.  With this configuration you can either
    * use `Optional` or `Option`, or a standard nullable Java type and get back a schema that allows nulls.
    *
    * If you need to mix nullable and non-nullable types, you may override the nullability of the type by either setting
    * a `NotNull` annotation on the given property, or setting the `required` attribute of the `JsonProperty` annotation.
    */
  val nullableJsonSchemaDraft4 = JsonSchemaConfig(
    autoGenerateTitleForProperties = false,
    defaultArrayFormat = None,
    useOneOfForOption = true,
    useOneOfForNullables = true,
    useNullableForOption = false,
    useNullableForNullables = false,
    usePropertyOrdering = false,
    hidePolymorphismTypeProperty = false,
    disableWarnings = false,
    useMinLengthForNotNull = false,
    useTypeIdForDefinitionName = false,
    customType2FormatMapping = Map(),
    useMultipleEditorSelectViaProperty = false,
    uniqueItemClasses = Set(),
    classTypeReMapping = Map(),
    jsonSuppliers = Map()
  )

  // Java-API
  def create(
      autoGenerateTitleForProperties: Boolean,
      defaultArrayFormat: Optional[String],
      useOneOfForOption: Boolean,
      useOneOfForNullables: Boolean,
      useNullableForOption: Boolean,
      useNullableForNullables: Boolean,
      usePropertyOrdering: Boolean,
      hidePolymorphismTypeProperty: Boolean,
      disableWarnings: Boolean,
      useMinLengthForNotNull: Boolean,
      useTypeIdForDefinitionName: Boolean,
      customType2FormatMapping: java.util.Map[String, String],
      useMultipleEditorSelectViaProperty: Boolean,
      uniqueItemClasses: java.util.Set[Class[_]],
      classTypeReMapping: java.util.Map[Class[_], Class[_]],
      jsonSuppliers: java.util.Map[String, Supplier[JsonNode]],
      subclassesResolver: SubclassesResolver,
      failOnUnknownProperties: Boolean,
      javaxValidationGroups: java.util.List[Class[_]]
  ): JsonSchemaConfig = {

    JsonSchemaConfig(
      autoGenerateTitleForProperties,
      Option(defaultArrayFormat.orElse(null)),
      useOneOfForOption,
      useOneOfForNullables,
      useNullableForOption,
      useNullableForNullables,
      usePropertyOrdering,
      hidePolymorphismTypeProperty,
      disableWarnings,
      useMinLengthForNotNull,
      useTypeIdForDefinitionName,
      customType2FormatMapping.asScala.toMap,
      useMultipleEditorSelectViaProperty,
      uniqueItemClasses.asScala.toSet,
      classTypeReMapping.asScala.toMap,
      jsonSuppliers.asScala.toMap,
      Option(subclassesResolver).getOrElse(new SubclassesResolverImpl()),
      failOnUnknownProperties,
      if (javaxValidationGroups == null) Array[Class[_]]()
      else {
        javaxValidationGroups.toArray.asInstanceOf[Array[Class[_]]]
      }
    )
  }

}

trait SubclassesResolver {
  def getSubclasses(clazz: Class[_]): List[Class[_]]
}

case class SubclassesResolverImpl(
    classGraph: Option[ClassGraph] = None,
    packagesToScan: List[String] = List(),
    classesToScan: List[String] = List()
) extends SubclassesResolver {

  def this() = this(None, List(), List())

  def withClassGraph(classGraph: ClassGraph): SubclassesResolverImpl = {
    this.copy(classGraph = Option(classGraph))
  }

  // Scala API
  def withPackagesToScan(packagesToScan: List[String]): SubclassesResolverImpl = {
    this.copy(packagesToScan = packagesToScan)
  }

  // Java API
  def withPackagesToScan(packagesToScan: JList[String]): SubclassesResolverImpl = {
    this.copy(packagesToScan = packagesToScan.asScala.toList)
  }

  // Scala API
  def withClassesToScan(classesToScan: List[String]): SubclassesResolverImpl = {
    this.copy(classesToScan = classesToScan)
  }

  // Java API
  def withClassesToScan(classesToScan: JList[String]): SubclassesResolverImpl = {
    this.copy(classesToScan = classesToScan.asScala.toList)
  }

  lazy val reflection: ScanResult = {

    var classGraphConfigured: Boolean = false

    if (classGraph.isDefined) {
      classGraphConfigured = true
    }

    val _classGraph: ClassGraph = classGraph.getOrElse(new ClassGraph())

    if (packagesToScan.nonEmpty) {
      classGraphConfigured = true
      _classGraph.acceptClasses(packagesToScan: _*)
    }

    if (classesToScan.nonEmpty) {
      classGraphConfigured = true
      _classGraph.acceptClasses(classesToScan: _*)
    }

    if (!classGraphConfigured) {
      LoggerFactory
        .getLogger(this.getClass)
        .warn(
          s"Performance-warning. Since SubclassesResolver is not configured," +
            s" it scans the entire classpath. " +
            s"https://github.com/mbknor/mbknor-jackson-jsonSchema#subclass-resolving-using-reflection"
        )
    }

    _classGraph.enableClassInfo().scan()
  }

  override def getSubclasses(clazz: Class[_]): List[Class[_]] = {
    if (clazz.isInterface)
      reflection.getClassesImplementing(clazz.getName).loadClasses().asScala.toList
    else
      reflection.getSubclasses(clazz.getName).loadClasses().asScala.toList
  }
}

case class JsonSchemaConfig(
    autoGenerateTitleForProperties: Boolean,
    defaultArrayFormat: Option[String],
    useOneOfForOption: Boolean,
    useOneOfForNullables: Boolean,
    useNullableForOption: Boolean,
    useNullableForNullables: Boolean,
    usePropertyOrdering: Boolean,
    hidePolymorphismTypeProperty: Boolean,
    disableWarnings: Boolean,
    useMinLengthForNotNull: Boolean,
    useTypeIdForDefinitionName: Boolean,
    customType2FormatMapping: Map[String, String],
    useMultipleEditorSelectViaProperty: Boolean, // https://github.com/jdorn/json-editor/issues/709
    uniqueItemClasses: Set[
      Class[_]
    ], // If rendering array and type is instanceOf class in this set, then we add 'uniqueItems": true' to schema - See // https://github.com/jdorn/json-editor for more info
    classTypeReMapping: Map[Class[_], Class[
      _
    ]], // Can be used to prevent rendering using polymorphism for specific classes.
    jsonSuppliers: Map[String, Supplier[
      JsonNode
    ]], // Suppliers in this map can be accessed using @JsonSchemaInject(jsonSupplierViaLookup = "lookupKey")
    subclassesResolver: SubclassesResolver =
      new SubclassesResolverImpl(), // Using default impl that scans entire classpath
    failOnUnknownProperties: Boolean = true,
    javaxValidationGroups: Array[Class[_]] =
      Array(), // Used to match against different validation-groups (javax.validation.constraints)
    jsonSchemaDraft: JsonSchemaDraft = JsonSchemaDraft.DRAFT_04
) {

  def withFailOnUnknownProperties(failOnUnknownProperties: Boolean): JsonSchemaConfig = {
    this.copy(failOnUnknownProperties = failOnUnknownProperties)
  }

  def withSubclassesResolver(subclassesResolver: SubclassesResolver): JsonSchemaConfig = {
    this.copy(subclassesResolver = subclassesResolver)
  }

  def withJavaxValidationGroups(javaxValidationGroups: Array[Class[_]]): JsonSchemaConfig = {
    this.copy(javaxValidationGroups = javaxValidationGroups)
  }

  def withJsonSchemaDraft(jsonSchemaDraft: JsonSchemaDraft): JsonSchemaConfig = {
    this.copy(jsonSchemaDraft = jsonSchemaDraft)
  }
}

/**
  * Json Schema Generator
  * @param rootObjectMapper pre-configured ObjectMapper
  * @param debug Default = false - set to true if generator should log some debug info while generating the schema
  * @param config default = vanillaJsonSchemaDraft4. Please use html5EnabledSchema if generating HTML5 GUI, e.g. using https://github.com/jdorn/json-editor
  */
class JsonSchemaGenerator(
    val rootObjectMapper: ObjectMapper,
    debug: Boolean = false,
    config: JsonSchemaConfig = JsonSchemaConfig.vanillaJsonSchemaDraft4
) {

  val javaxValidationGroups = config.javaxValidationGroups

  // Java API
  def this(rootObjectMapper: ObjectMapper) =
    this(rootObjectMapper, false, JsonSchemaConfig.vanillaJsonSchemaDraft4)

  // Java API
  def this(rootObjectMapper: ObjectMapper, config: JsonSchemaConfig) =
    this(rootObjectMapper, false, config)

  val log = LoggerFactory.getLogger(getClass)

  val dateFormatMapping = Map[String, String](
    // Java7 dates
    "java.time.LocalDateTime" -> "datetime-local",
    "java.time.OffsetDateTime" -> "datetime",
    "java.time.LocalDate" -> "date",
    // Joda-dates
    "org.joda.time.LocalDate" -> "date"
  )

  trait MySerializerProvider {
    var provider: SerializerProvider = null

    def getProvider: SerializerProvider = provider
    def setProvider(provider: SerializerProvider): Unit = this.provider = provider
  }

  trait EnumSupport {

    val _node: ObjectNode

    def enumTypes(enums: util.Set[String]): Unit = {
      // l(s"JsonStringFormatVisitor-enum.enumTypes: ${enums}")

      val enumValuesNode = JsonNodeFactory.instance.arrayNode()
      _node.set("enum", enumValuesNode)

      enums.asScala.foreach { enumValue =>
        if (enumValue.nonEmpty) {
          enumValuesNode.add(enumValue)
        }
      }
    }
  }

  private def setFormat(node: ObjectNode, format: String): Unit = {
    node.put("format", format)
  }

  // Verifies that the annotation is applicable based on the config.javaxValidationGroups
  private def annotationIsApplicable(annotation: Annotation): Boolean = {

    def extractGroupsFromAnnotation(annotation: Annotation): Array[Class[_]] = {
      // Annotations cannot implement interface, so we have to check each and every
      // javax-annotation... To prevent bugs with missing groups-extract-impl when new
      // validation-annotations are added, I've decided to do it using reflection
      val annotationClass = annotation.annotationType()
      if (annotationClass.getPackage.getName().startsWith("javax.validation.constraints")) {
        val groupsMethod =
          try {
            annotationClass.getMethod("groups")
          } catch {
            case e: NoSuchMethodException => null
          }
        if (groupsMethod != null) {
          groupsMethod.invoke(annotation).asInstanceOf[Array[Class[_]]]
        } else {
          Array()
        }
      } else {
        annotation match {
          case x: JsonSchemaInject => x.javaxValidationGroups()
          case _                   => Array()
        }
      }
    }

    val javaxDefaultGroup = classOf[Default]

    val groupsOnAnnotation: Array[Class[_]] = extractGroupsFromAnnotation(annotation)

    (javaxValidationGroups, groupsOnAnnotation) match {
      case (Array(), Array()) => true
      case (Array(), l) =>
        l.contains(javaxDefaultGroup) // Use it if groupsOnAnnotation contains Default
      case (l, Array()) =>
        l.contains(javaxDefaultGroup) // Use it if javaxValidationGroups contains Default
      case (a, b) => a.exists(c => b.contains(c)) // One of a must be included in b
    }
  }

  // Tries to retrieve a annotation and validates that it is applicable
  private def selectAnnotation[T <: Annotation](
      property: BeanProperty,
      annotationClass: Class[T]
  ): Option[T] = {
    Option(property.getAnnotation(annotationClass))
      .filter(annotationIsApplicable(_))
  }

  // Tries to retrieve a annotation and validates that it is applicable
  private def selectAnnotation[T <: Annotation](
      annotatedClass: AnnotatedClass,
      annotationClass: Class[T]
  ): Option[T] = {
    Option(annotatedClass.getAnnotation(annotationClass))
      .filter(annotationIsApplicable(_))
  }

  case class DefinitionInfo(
      ref: Option[String],
      jsonObjectFormatVisitor: Option[JsonObjectFormatVisitor]
  )

  // Class that manages creating new definitions or getting $refs to existing definitions
  class DefinitionsHandler() {
    private var class2Ref = Map[JavaType, String]()
    private val definitionsNode = JsonNodeFactory.instance.objectNode()

    case class WorkInProgress(typeInProgress: JavaType, nodeInProgress: ObjectNode)

    // Used when 'combining' multiple invocations to getOrCreateDefinition when processing polymorphism.
    private var workInProgress: Option[WorkInProgress] = None

    private var workInProgressStack = List[Option[WorkInProgress]]()

    def pushWorkInProgress(): Unit = {
      workInProgressStack = workInProgress :: workInProgressStack
      workInProgress = None
    }

    def popworkInProgress(): Unit = {
      workInProgress = workInProgressStack.head
      workInProgressStack = workInProgressStack.tail
    }

    def extractTypeName(_type: JavaType): String = {
      // use JsonTypeName annotation if present
      val annotation = _type.getRawClass.getDeclaredAnnotation(classOf[JsonTypeName])
      Option(annotation)
        .flatMap(a => Option(a.value()))
        .filter(_.nonEmpty)
        .getOrElse(_type.getRawClass.getSimpleName)
    }

    def getDefinitionName(_type: JavaType): String = {
      val baseName =
        if (config.useTypeIdForDefinitionName) _type.getRawClass.getTypeName
        else extractTypeName(_type)

      if (_type.hasGenericTypes) {
        val containedTypes = Range(0, _type.containedTypeCount()).map(_type.containedType)
        val typeNames = containedTypes.map(getDefinitionName).mkString(",")
        s"$baseName($typeNames)"
      } else {
        baseName
      }
    }

    // Either creates new definitions or return $ref to existing one
    def getOrCreateDefinition(
        _type: JavaType
    )(objectDefinitionBuilder: (ObjectNode) => Option[JsonObjectFormatVisitor]): DefinitionInfo = {

      class2Ref.get(_type) match {
        case Some(ref) =>
          workInProgress match {
            case None =>
              DefinitionInfo(Some(ref), None)

            case Some(w) =>
              // this is a recursive polymorphism call
              if (_type != w.typeInProgress)
                throw new Exception(s"Wrong type - working on ${w.typeInProgress} - got ${_type}")

              DefinitionInfo(None, objectDefinitionBuilder(w.nodeInProgress))
          }

        case None =>
          // new one - must build it
          var retryCount = 0
          val definitionName = getDefinitionName(_type)
          var shortRef = definitionName
          var longRef = "#/definitions/" + definitionName
          while (class2Ref.values.toList.contains(longRef)) {
            retryCount = retryCount + 1
            shortRef = definitionName + "_" + retryCount
            longRef = "#/definitions/" + definitionName + "_" + retryCount
          }
          class2Ref = class2Ref + (_type -> longRef)

          // create definition
          val node = JsonNodeFactory.instance.objectNode()

          // When processing polymorphism, we might get multiple recursive calls to getOrCreateDefinition - this is a wau to combine them
          workInProgress = Some(WorkInProgress(_type, node))

          definitionsNode.set(shortRef, node)

          val jsonObjectFormatVisitor = objectDefinitionBuilder.apply(node)

          workInProgress = None

          DefinitionInfo(Some(longRef), jsonObjectFormatVisitor)
      }
    }

    def getFinalDefinitionsNode(): Option[ObjectNode] = {
      if (class2Ref.isEmpty) None else Some(definitionsNode)
    }

  }

  class MyJsonFormatVisitorWrapper(
      objectMapper: ObjectMapper,
      level: Int = 0,
      val node: ObjectNode = JsonNodeFactory.instance.objectNode(),
      val definitionsHandler: DefinitionsHandler,
      currentProperty: Option[
        BeanProperty
      ] // This property may represent the BeanProperty when we're directly processing beneath the property
  ) extends JsonFormatVisitorWrapper
      with MySerializerProvider {

    def l(s: => String): Unit = {
      if (!debug) return

      var indent = ""
      for (_ <- 0 until level) {
        indent = indent + "  "
      }
      println(indent + s)
    }

    def createChild(
        childNode: ObjectNode,
        currentProperty: Option[BeanProperty]
    ): MyJsonFormatVisitorWrapper = {
      new MyJsonFormatVisitorWrapper(
        objectMapper,
        level + 1,
        node = childNode,
        definitionsHandler = definitionsHandler,
        currentProperty = currentProperty
      )
    }

    def extractDefaultValue(p: BeanProperty): Option[String] = {
      // Prefer default-value from @JsonProperty
      selectAnnotation(p, classOf[JsonProperty])
        .flatMap { jsonProp =>
          val defaultValue = jsonProp.defaultValue();
          // Since it is default set to "", we should only use it if it is nonEmpty
          if (defaultValue.nonEmpty) {
            Some(defaultValue)
          } else None
        }
        .orElse {
          // Then, look for @JsonSchemaDefault
          selectAnnotation(p, classOf[JsonSchemaDefault]).map { defaultValue =>
            defaultValue.value()
          }
        }
    }

    override def expectStringFormat(_type: JavaType) = {
      l(s"expectStringFormat - _type: ${_type}")

      node.put("type", "string")

      // Check if we should include minLength and/or maxLength
      case class MinAndMaxLength(minLength: Option[Int], maxLength: Option[Int])

      // If we have 'currentProperty', then check for annotations and insert stuff into schema.
      currentProperty.flatMap { p =>
        // Look for @NotBlank
        selectAnnotation(p, classOf[NotBlank]).map { _ =>
          // Need to write this pattern first in case we should override it with more specific @Pattern
          node.put("pattern", "^.*\\S+.*$")
        }

        // Look for @Pattern
        selectAnnotation(p, classOf[Pattern]).map { pattern =>
          node.put("pattern", pattern.regexp())
        }

        // Look for @Pattern.List
        selectAnnotation(p, classOf[Pattern.List]).map { patterns =>
          {
            val regex =
              patterns.value().map(_.regexp).foldLeft("^")(_ + "(?=" + _ + ")").concat(".*$")
            node.put("pattern", regex)
          }
        }

        extractDefaultValue(p).map { value =>
          node.put("default", value)
        }

        // Look for @JsonSchemaExamples
        selectAnnotation(p, classOf[JsonSchemaExamples]).map { exampleValues =>
          val examples: ArrayNode = JsonNodeFactory.instance.arrayNode()
          exampleValues.value().map { exampleValue =>
            examples.add(exampleValue)
          }
          node.set("examples", examples)
          ()
        }

        // Look for @Email
        selectAnnotation(p, classOf[Email]).map { _ =>
          node.put("format", "email")
        }

        // Look for a @Size annotation, which should have a set of min/max properties.
        val minAndMaxLength: Option[MinAndMaxLength] = selectAnnotation(p, classOf[Size])
          .map { size =>
            (size.min(), size.max()) match {
              case (0, max)                 => MinAndMaxLength(None, Some(max))
              case (min, Integer.MAX_VALUE) => MinAndMaxLength(Some(min), None)
              case (min, max)               => MinAndMaxLength(Some(min), Some(max))
            }
          }
          // Look for other annotations that don't have an explicit size, but we can infer the need to set a size for.
          .orElse {
            // If we're annotated with @NotNull, check to see if our config requires a size property to be generated.
            if (
              config.useMinLengthForNotNull && (selectAnnotation(p, classOf[NotNull]).isDefined)
            ) {
              Option(MinAndMaxLength(Some(1), None))
            }
            // Other javax.validation annotations that require a length.
            else if (
              selectAnnotation(p, classOf[NotBlank]).isDefined || selectAnnotation(
                p,
                classOf[NotEmpty]
              ).isDefined
            ) {
              Option(MinAndMaxLength(Some(1), None))
            }
            // No length required.
            else {
              None
            }
          }

        // Apply size-data if found
        minAndMaxLength.map { minAndMax: MinAndMaxLength =>
          minAndMax.minLength.map(length => node.put("minLength", length))
          minAndMax.maxLength.map(length => node.put("maxLength", length))
        }
      }

      new JsonStringFormatVisitor with EnumSupport {
        val _node = node
        override def format(format: JsonValueFormat): Unit = {
          setFormat(node, format.toString)
        }
      }

    }

    override def expectArrayFormat(_type: JavaType) = {
      l(s"expectArrayFormat - _type: ${_type}")

      node.put("type", "array")

      if (config.uniqueItemClasses.exists(c => _type.getRawClass.isAssignableFrom(c))) {
        // Adding '"uniqueItems": true' to be used with https://github.com/jdorn/json-editor
        node.put("uniqueItems", true)
        setFormat(node, "checkbox")
      } else {
        // Try to set default format
        config.defaultArrayFormat.foreach { format =>
          setFormat(node, format)
        }
      }

      currentProperty.map { p =>
        // Look for @Size
        selectAnnotation(p, classOf[Size]).map { size =>
          node.put("minItems", size.min())
          node.put("maxItems", size.max())
        }

        // Look for @NotEmpty
        selectAnnotation(p, classOf[NotEmpty]).map { notEmpty =>
          node.put("minItems", 1)
        }
      }

      val itemsNode = JsonNodeFactory.instance.objectNode()
      node.set("items", itemsNode)

      // We get improved result while processing scala-collections by getting elementType this way
      // instead of using the one which we receive in JsonArrayFormatVisitor.itemsFormat
      // This approach also works for Java
      val preferredElementType: JavaType = _type.getContentType

      new JsonArrayFormatVisitor with MySerializerProvider {
        override def itemsFormat(handler: JsonFormatVisitable, _elementType: JavaType): Unit = {
          l(
            s"expectArrayFormat - handler: $handler - elementType: ${_elementType} - preferredElementType: $preferredElementType"
          )
          objectMapper.acceptJsonFormatVisitor(
            tryToReMapType(preferredElementType),
            createChild(itemsNode, currentProperty = None)
          )
        }

        override def itemsFormat(format: JsonFormatTypes): Unit = {
          l(s"itemsFormat - format: $format")
          itemsNode.put("type", format.value())
        }
      }
    }

    override def expectNumberFormat(_type: JavaType) = {
      l("expectNumberFormat")

      node.put("type", "number")

      // Look for @Min, @Max, @DecimalMin, @DecimalMax => minimum, maximum
      currentProperty.map { p =>
        selectAnnotation(p, classOf[Min]).map { min =>
          node.put("minimum", min.value())
        }

        selectAnnotation(p, classOf[Max]).map { max =>
          node.put("maximum", max.value())
        }

        selectAnnotation(p, classOf[DecimalMin]).map { decimalMin =>
          node.put("minimum", decimalMin.value().toDouble)
        }

        selectAnnotation(p, classOf[DecimalMax]).map { decimalMax =>
          node.put("maximum", decimalMax.value().toDouble)
        }

        extractDefaultValue(p).map { value =>
          node.put("default", value.toInt)
        }

        // Look for @JsonSchemaExamples
        Option(p.getAnnotation(classOf[JsonSchemaExamples])).map { exampleValues =>
          val examples: ArrayNode = JsonNodeFactory.instance.arrayNode()
          exampleValues.value().map { exampleValue =>
            examples.add(exampleValue)
          }
          node.set("examples", examples)
        }
      }

      new JsonNumberFormatVisitor with EnumSupport {
        val _node = node
        override def numberType(_type: NumberType): Unit =
          l(s"JsonNumberFormatVisitor.numberType: ${_type}")
        override def format(format: JsonValueFormat): Unit = {
          setFormat(node, format.toString)
        }
      }
    }

    override def expectAnyFormat(_type: JavaType) = {
      if (!config.disableWarnings) {
        log.warn(
          s"Not able to generate jsonSchema-info for type: ${_type} - probably using custom serializer which does not override acceptJsonFormatVisitor"
        )
      }

      new JsonAnyFormatVisitor {}

    }

    override def expectIntegerFormat(_type: JavaType) = {
      l("expectIntegerFormat")

      node.put("type", "integer")

      // Look for @Min, @Max => minimum, maximum
      currentProperty.map { p =>
        selectAnnotation(p, classOf[Min]).map { min =>
          node.put("minimum", min.value())
        }

        selectAnnotation(p, classOf[Max]).map { max =>
          node.put("maximum", max.value())
        }

        extractDefaultValue(p).map { value =>
          node.put("default", value.toInt)
        }

        // Look for @JsonSchemaExamples
        selectAnnotation(p, classOf[JsonSchemaExamples]).map { exampleValues =>
          val examples: ArrayNode = JsonNodeFactory.instance.arrayNode()
          exampleValues.value().map { exampleValue =>
            examples.add(exampleValue)
          }
          node.set("examples", examples)
          ()
        }
      }

      new JsonIntegerFormatVisitor with EnumSupport {
        val _node = node
        override def numberType(_type: NumberType): Unit =
          l(s"JsonIntegerFormatVisitor.numberType: ${_type}")
        override def format(format: JsonValueFormat): Unit = {
          setFormat(node, format.toString)
        }
      }
    }

    override def expectNullFormat(_type: JavaType) = {
      l(s"expectNullFormat - _type: ${_type}")
      node.put("type", "null")
      new JsonNullFormatVisitor {}
    }

    override def expectBooleanFormat(_type: JavaType) = {
      l("expectBooleanFormat")

      node.put("type", "boolean")

      currentProperty.map { p =>
        extractDefaultValue(p).map { value =>
          node.put("default", value.toBoolean)
        }
      }

      new JsonBooleanFormatVisitor with EnumSupport {
        val _node = node
        override def format(format: JsonValueFormat): Unit = {
          setFormat(node, format.toString)
        }
      }
    }

    override def expectMapFormat(_type: JavaType) = {
      l(s"expectMapFormat - _type: ${_type}")

      // There is no way to specify map in jsonSchema,
      // So we're going to treat it as type=object with additionalProperties = true,
      // so that it can hold whatever the map can hold

      node.put("type", "object")

      val additionalPropsObject = JsonNodeFactory.instance.objectNode()
      node.set("additionalProperties", additionalPropsObject)

      // If we're annotated with @NotEmpty, make sure we add a minItems of 1 to our schema here.
      currentProperty.map { p =>
        Option(p.getAnnotation(classOf[NotEmpty])).map { notEmpty =>
          node.put("minProperties", 1)
        }
      }

      definitionsHandler.pushWorkInProgress()

      val childVisitor = createChild(additionalPropsObject, None)
      objectMapper.acceptJsonFormatVisitor(tryToReMapType(_type.getContentType), childVisitor)
      definitionsHandler.popworkInProgress()

      new JsonMapFormatVisitor with MySerializerProvider {
        override def keyFormat(handler: JsonFormatVisitable, keyType: JavaType): Unit = {
          l(s"JsonMapFormatVisitor.keyFormat handler: $handler - keyType: $keyType")
        }

        override def valueFormat(handler: JsonFormatVisitable, valueType: JavaType): Unit = {
          l(s"JsonMapFormatVisitor.valueFormat handler: $handler - valueType: $valueType")
        }
      }
    }

    private def getRequiredArrayNode(objectNode: ObjectNode): ArrayNode = {
      Option(objectNode.get("required")).map(_.asInstanceOf[ArrayNode]).getOrElse {
        val rn = JsonNodeFactory.instance.arrayNode()
        objectNode.set("required", rn)
        rn
      }
    }

    private def getOptionsNode(objectNode: ObjectNode): ObjectNode = {
      getOrCreateObjectChild(objectNode, "options")
    }

    private def getOrCreateObjectChild(parentObjectNode: ObjectNode, name: String): ObjectNode = {
      Option(parentObjectNode.get(name)).map(_.asInstanceOf[ObjectNode]).getOrElse {
        val o = JsonNodeFactory.instance.objectNode()
        parentObjectNode.set(name, o)
        o
      }
    }

    case class PolymorphismInfo(typePropertyName: String, subTypeName: String)

    private def extractPolymorphismInfo(_type: JavaType): Option[PolymorphismInfo] = {
      val maybeBaseType = ClassUtil.findSuperTypes(_type, null, false).asScala.find { cl =>
        cl.getRawClass.isAnnotationPresent(classOf[JsonTypeInfo])
      } orElse Option(_type.getSuperClass)

      maybeBaseType.flatMap { baseType =>
        val serializerOrNull = objectMapper.getSerializerFactory
          .createTypeSerializer(objectMapper.getSerializationConfig, baseType)

        Option(serializerOrNull).map { serializer =>
          serializer.getTypeInclusion match {
            case JsonTypeInfo.As.PROPERTY | JsonTypeInfo.As.EXISTING_PROPERTY =>
              val idResolver = serializer.getTypeIdResolver
              val id = idResolver match {
                // use custom implementation instead, because default implementation needs instance and we don't have one
                case _: MinimalClassNameIdResolver => extractMinimalClassnameId(baseType, _type)
                case _                             => idResolver.idFromValueAndType(null, _type.getRawClass)
              }
              PolymorphismInfo(serializer.getPropertyName, id)

            case x =>
              throw new Exception(
                s"We do not support polymorphism using jsonTypeInfo.include() = $x"
              )
          }
        }
      }
    }

    private def extractSubTypes(_type: JavaType): List[Class[_]] = {

      val ac = AnnotatedClassResolver.resolve(
        objectMapper.getDeserializationConfig,
        _type,
        objectMapper.getDeserializationConfig
      )

      Option(ac.getAnnotation(classOf[JsonTypeInfo]))
        .map { jsonTypeInfo: JsonTypeInfo =>
          jsonTypeInfo.use() match {
            case JsonTypeInfo.Id.NAME =>
              // First we try to resolve types via manually finding annotations (if success, it will preserve the order), if not we fallback to use collectAndResolveSubtypesByClass()
              val subTypes: List[Class[_]] =
                Option(_type.getRawClass.getDeclaredAnnotation(classOf[JsonSubTypes]))
                  .map { ann: JsonSubTypes =>
                    // We found it via @JsonSubTypes-annotation
                    ann
                      .value()
                      .map { t: JsonSubTypes.Type =>
                        t.value()
                      }
                      .toList
                  }
                  .getOrElse {
                    // We did not find it via @JsonSubTypes-annotation (Probably since it is using mixin's) => Must fallback to using collectAndResolveSubtypesByClass
                    val resolvedSubTypes = objectMapper.getSubtypeResolver
                      .collectAndResolveSubtypesByClass(objectMapper.getDeserializationConfig, ac)
                      .asScala
                      .toList
                    resolvedSubTypes
                      .map(_.getType)
                      .filter(c => _type.getRawClass.isAssignableFrom(c) && _type.getRawClass != c)
                  }

              subTypes

            case _ =>
              // Just find all subclasses
              config.subclassesResolver.getSubclasses(_type.getRawClass)
          }

        }
        .getOrElse(List())
    }

    def tryToReMapType(originalClass: Class[_]): Class[_] = {
      config.classTypeReMapping
        .get(originalClass)
        .map { mappedToClass: Class[_] =>
          l(s"Class $originalClass is remapped to $mappedToClass")
          mappedToClass
        }
        .getOrElse(originalClass)
    }

    private def tryToReMapType(originalType: JavaType): JavaType = {
      val _type: JavaType = config.classTypeReMapping
        .get(originalType.getRawClass)
        .map { mappedToClass: Class[_] =>
          l(s"Class ${originalType.getRawClass} is remapped to $mappedToClass")
          val mappedToJavaType: JavaType = objectMapper.getTypeFactory.constructType(mappedToClass)
          mappedToJavaType
        }
        .getOrElse(originalType)

      _type
    }

    // Returns the value of merge
    private def injectFromJsonSchemaInject(
        a: JsonSchemaInject,
        thisObjectNode: ObjectNode
    ): Boolean = {
      // Must parse json
      val injectJsonNode = objectMapper.readTree(a.json())
      Option(a.jsonSupplier())
        .flatMap(cls => Option(cls.getDeclaredConstructor().newInstance().get()))
        .foreach(json => merge(injectJsonNode, json))
      if (a.jsonSupplierViaLookup().nonEmpty) {
        val json = config.jsonSuppliers
          .getOrElse(
            a.jsonSupplierViaLookup(),
            throw new Exception(
              s"@JsonSchemaInject(jsonSupplierLookup='${a.jsonSupplierViaLookup()}') does not exist in config.jsonSupplierLookup-map"
            )
          )
          .get()
        merge(injectJsonNode, json)
      }
      a.strings().foreach(v => injectJsonNode.visit(v.path(), (o, n) => o.put(n, v.value())))
      a.ints().foreach(v => injectJsonNode.visit(v.path(), (o, n) => o.put(n, v.value())))
      a.bools().foreach(v => injectJsonNode.visit(v.path(), (o, n) => o.put(n, v.value())))

      val mergeInjectedJson: Boolean = a.merge()
      if (!mergeInjectedJson) {
        // Since we're not merging, we must remove all content of thisObjectNode before injecting.
        // We cannot just "replace" it with injectJsonNode, since thisObjectNode already have been added to its parent
        thisObjectNode.removeAll()
      }

      merge(thisObjectNode, injectJsonNode)

      // return
      mergeInjectedJson
    }

    override def expectObjectFormat(_type: JavaType) = {

      val subTypes: List[Class[_]] = extractSubTypes(_type)

      // Check if we have subtypes
      if (subTypes.nonEmpty) {
        // We have subtypes
        //l(s"polymorphism - subTypes: $subTypes")

        val anyOfArrayNode = JsonNodeFactory.instance.arrayNode()
        node.set("oneOf", anyOfArrayNode)

        subTypes.foreach { subType: Class[_] =>
          l(s"polymorphism - subType: $subType")
          val definitionInfo: DefinitionInfo =
            definitionsHandler.getOrCreateDefinition(objectMapper.constructType(subType)) {
              objectNode =>
                val childVisitor = createChild(objectNode, currentProperty = None)
                objectMapper.acceptJsonFormatVisitor(tryToReMapType(subType), childVisitor)

                None
            }

          val thisOneOfNode = JsonNodeFactory.instance.objectNode()
          thisOneOfNode.put("$ref", definitionInfo.ref.get)

          // If class is annotated with JsonSchemaTitle, we should add it
          Option(subType.getDeclaredAnnotation(classOf[JsonSchemaTitle])).map(_.value()).foreach {
            title =>
              thisOneOfNode.put("title", title)
          }

          anyOfArrayNode.add(thisOneOfNode)

        }

        null // Returning null to stop jackson from visiting this object since we have done it manually

      } else {
        // We do not have subtypes

        val objectBuilder: ObjectNode => Option[JsonObjectFormatVisitor] = {
          thisObjectNode: ObjectNode =>
            thisObjectNode.put("type", "object")
            thisObjectNode.put("additionalProperties", !config.failOnUnknownProperties)

            // If class is annotated with JsonSchemaFormat, we should add it
            val ac = AnnotatedClassResolver.resolve(
              objectMapper.getDeserializationConfig,
              _type,
              objectMapper.getDeserializationConfig
            )
            resolvePropertyFormat(_type, objectMapper).foreach { format =>
              setFormat(thisObjectNode, format)
            }

            // If class is annotated with JsonSchemaDescription, we should add it
            Option(ac.getAnnotations.get(classOf[JsonSchemaDescription]))
              .map(_.value())
              .orElse(Option(ac.getAnnotations.get(classOf[JsonPropertyDescription])).map(_.value))
              .foreach { description: String =>
                thisObjectNode.put("description", description)
              }

            // If class is annotated with JsonSchemaTitle, we should add it
            Option(ac.getAnnotations.get(classOf[JsonSchemaTitle])).map(_.value()).foreach {
              title =>
                thisObjectNode.put("title", title)
            }

            // If class is annotated with JsonSchemaOptions, we should add it
            Option(ac.getAnnotations.get(classOf[JsonSchemaOptions])).map(_.items()).foreach {
              items =>
                val optionsNode = getOptionsNode(thisObjectNode)
                items.foreach { item =>
                  optionsNode.put(item.name, item.value)
                }
            }

            // Optionally add JsonSchemaInject to top-level
            val renderProps: Boolean = selectAnnotation(ac, classOf[JsonSchemaInject])
              .map { a =>
                val merged = injectFromJsonSchemaInject(a, thisObjectNode)
                merged == true // Continue to render props since we merged injection
              }
              .getOrElse(true) // nothing injected => of course we should render props

            if (renderProps) {

              val propertiesNode = getOrCreateObjectChild(thisObjectNode, "properties")

              extractPolymorphismInfo(_type).map {
                case pi: PolymorphismInfo =>
                  // This class is a child in a polymorphism config..
                  // Set the title = subTypeName
                  thisObjectNode.put("title", pi.subTypeName)

                  // must inject the 'type'-param and value as enum with only one possible value
                  // This is done to make sure the json generated from the schema using this oneOf
                  // contains the correct "type info"
                  val enumValuesNode = JsonNodeFactory.instance.arrayNode()
                  enumValuesNode.add(pi.subTypeName)

                  val enumObjectNode = getOrCreateObjectChild(propertiesNode, pi.typePropertyName)
                  enumObjectNode.put("type", "string")
                  enumObjectNode.set("enum", enumValuesNode)
                  enumObjectNode.put("default", pi.subTypeName)

                  if (config.hidePolymorphismTypeProperty) {
                    // Make sure the editor hides this polymorphism-specific property
                    val optionsNode = JsonNodeFactory.instance.objectNode()
                    enumObjectNode.set("options", optionsNode)
                    optionsNode.put("hidden", true)
                  }

                  getRequiredArrayNode(thisObjectNode).add(pi.typePropertyName)

                  if (config.useMultipleEditorSelectViaProperty) {
                    // https://github.com/jdorn/json-editor/issues/709
                    // Generate info to help generated editor to select correct oneOf-type
                    // when populating the gui/schema with existing data
                    val objectOptionsNode = getOrCreateObjectChild(thisObjectNode, "options")
                    val multipleEditorSelectViaPropertyNode = getOrCreateObjectChild(
                      objectOptionsNode,
                      "multiple_editor_select_via_property"
                    )
                    multipleEditorSelectViaPropertyNode.put("property", pi.typePropertyName)
                    multipleEditorSelectViaPropertyNode.put("value", pi.subTypeName)
                    ()
                  }

              }

              Some(new JsonObjectFormatVisitor with MySerializerProvider {

                // Used when rendering schema using propertyOrdering as specified here:
                // https://github.com/jdorn/json-editor#property-ordering
                var nextPropertyOrderIndex = 1

                def myPropertyHandler(
                    propertyName: String,
                    propertyType: JavaType,
                    prop: Option[BeanProperty],
                    jsonPropertyRequired: Boolean
                ): Unit = {
                  l(s"JsonObjectFormatVisitor - ${propertyName}: ${propertyType}")

                  if (propertiesNode.get(propertyName) != null) {
                    if (!config.disableWarnings) {
                      log.warn(
                        s"Ignoring property '$propertyName' in $propertyType since it has already been added, probably as type-property using polymorphism"
                      )
                    }
                    return
                  }

                  // Need to check for Option/Optional-special-case before we know what node to use here.
                  case class PropertyNode(main: ObjectNode, meta: ObjectNode)

                  // Check if we should set this property as required. Primitive types MUST have a value, as does anything
                  // with a @JsonProperty that has "required" set to true. Lastly, various javax.validation annotations also
                  // make this required.
                  val requiredProperty: Boolean =
                    if (
                      propertyType.getRawClass.isPrimitive || jsonPropertyRequired || validationAnnotationRequired(
                        prop
                      )
                    ) {
                      true
                    } else {
                      false
                    }

                  val thisPropertyNode: PropertyNode = {
                    val thisPropertyNode = JsonNodeFactory.instance.objectNode()
                    propertiesNode.set(propertyName, thisPropertyNode)

                    if (config.usePropertyOrdering) {
                      thisPropertyNode.put("propertyOrder", nextPropertyOrderIndex)
                      nextPropertyOrderIndex = nextPropertyOrderIndex + 1
                    }

                    // Figure out if the type is considered optional by either Java or Scala.
                    val optionalType: Boolean =
                      classOf[Option[_]].isAssignableFrom(propertyType.getRawClass) ||
                        classOf[Optional[_]].isAssignableFrom(propertyType.getRawClass)

                    // If the property is not required, and our configuration allows it, let's go ahead and mark the type as nullable.
                    if (
                      !requiredProperty && ((config.useOneOfForOption && optionalType) ||
                      (config.useOneOfForNullables && !optionalType))
                    ) {
                      // We support this type being null, insert a oneOf consisting of a sentinel "null" and the real type.
                      val oneOfArray = JsonNodeFactory.instance.arrayNode()
                      thisPropertyNode.set("oneOf", oneOfArray)

                      // Create our sentinel "null" value for the case no value is provided.
                      val oneOfNull = JsonNodeFactory.instance.objectNode()
                      oneOfNull.put("type", "null")
                      oneOfNull.put("title", "Not included")
                      oneOfArray.add(oneOfNull)

                      // If our nullable/optional type has a value, it'll be this.
                      val oneOfReal = JsonNodeFactory.instance.objectNode()
                      oneOfArray.add(oneOfReal)

                      // Return oneOfReal which, from now on, will be used as the node representing this property
                      PropertyNode(oneOfReal, thisPropertyNode)
                    } else if (
                      !requiredProperty && ((config.useNullableForOption && optionalType) ||
                      (config.useNullableForNullables && !optionalType))
                    ) {
                      // add {nullable: true} in the json schema following OpenAPI and AJV specification
                      // see https://ajv.js.org/json-schema.html#openapi-support
                      thisPropertyNode.put("nullable", true)
                      PropertyNode(thisPropertyNode, thisPropertyNode)
                    } else {
                      // Our type must not be null: primitives, @NotNull annotations, @JsonProperty annotations marked required etc.
                      PropertyNode(thisPropertyNode, thisPropertyNode)
                    }
                  }

                  // Continue processing this property
                  val childVisitor = createChild(thisPropertyNode.main, currentProperty = prop)

                  // Push current work in progress since we're about to start working on a new class
                  definitionsHandler.pushWorkInProgress()

                  if (
                    (classOf[Option[_]]
                      .isAssignableFrom(propertyType.getRawClass) || classOf[Optional[_]]
                      .isAssignableFrom(propertyType.getRawClass)) && propertyType
                      .containedTypeCount() >= 1
                  ) {

                    // Property is scala Option or Java Optional.
                    //
                    // Due to Java's Type Erasure, the type behind Option is lost.
                    // To workaround this, we use the same workaround as jackson-scala-module described here:
                    // https://github.com/FasterXML/jackson-module-scala/wiki/FAQ#deserializing-optionint-and-other-primitive-challenges

                    val optionType: JavaType = resolveType(propertyType, prop, objectMapper)

                    objectMapper.acceptJsonFormatVisitor(tryToReMapType(optionType), childVisitor)

                  } else {
                    objectMapper.acceptJsonFormatVisitor(tryToReMapType(propertyType), childVisitor)
                  }

                  // Pop back the work we were working on..
                  definitionsHandler.popworkInProgress()

                  prop.flatMap(resolvePropertyFormat(_)).foreach { format =>
                    setFormat(thisPropertyNode.main, format)
                  }

                  // Optionally add description
                  prop
                    .flatMap { p: BeanProperty =>
                      Option(p.getAnnotation(classOf[JsonSchemaDescription]))
                        .map(_.value())
                        .orElse(
                          Option(p.getAnnotation(classOf[JsonPropertyDescription])).map(_.value())
                        )
                    }
                    .map { description =>
                      thisPropertyNode.meta.put("description", description)
                    }

                  // If this property is required, add it to our array of required properties.
                  if (requiredProperty) {
                    getRequiredArrayNode(thisObjectNode).add(propertyName)
                  }

                  // Optionally add title
                  prop
                    .flatMap { p: BeanProperty =>
                      Option(p.getAnnotation(classOf[JsonSchemaTitle]))
                    }
                    .map(_.value())
                    .orElse {
                      if (config.autoGenerateTitleForProperties) {
                        // We should generate 'pretty-name' based on propertyName
                        Some(generateTitleFromPropertyName(propertyName))
                      } else None
                    }
                    .map { title =>
                      thisPropertyNode.meta.put("title", title)
                    }

                  // Optionally add options
                  prop
                    .flatMap { p: BeanProperty =>
                      Option(p.getAnnotation(classOf[JsonSchemaOptions]))
                    }
                    .map(_.items())
                    .foreach { items =>
                      val optionsNode = getOptionsNode(thisPropertyNode.meta)
                      items.foreach { item =>
                        optionsNode.put(item.name, item.value)

                      }
                    }

                  // Optionally add JsonSchemaInject
                  prop
                    .flatMap { p: BeanProperty =>
                      selectAnnotation(p, classOf[JsonSchemaInject]) match {
                        case Some(a) => Some(a)
                        case None    =>
                          // Try to look at the class itself -- Looks like this is the only way to find it if the type is Enum
                          Option(p.getType.getRawClass.getAnnotation(classOf[JsonSchemaInject]))
                            .filter(annotationIsApplicable(_))
                      }
                    }
                    .foreach { a =>
                      injectFromJsonSchemaInject(a, thisPropertyNode.meta)
                    }
                }

                override def optionalProperty(prop: BeanProperty): Unit = {
                  l(s"JsonObjectFormatVisitor.optionalProperty: prop:${prop}")
                  myPropertyHandler(
                    prop.getName,
                    prop.getType,
                    Some(prop),
                    jsonPropertyRequired = false
                  )
                }

                override def optionalProperty(
                    name: String,
                    handler: JsonFormatVisitable,
                    propertyTypeHint: JavaType
                ): Unit = {
                  l(
                    s"JsonObjectFormatVisitor.optionalProperty: name:${name} handler:${handler} propertyTypeHint:${propertyTypeHint}"
                  )
                  myPropertyHandler(name, propertyTypeHint, None, jsonPropertyRequired = false)
                }

                override def property(prop: BeanProperty): Unit = {
                  l(s"JsonObjectFormatVisitor.property: prop:${prop}")
                  myPropertyHandler(
                    prop.getName,
                    prop.getType,
                    Some(prop),
                    jsonPropertyRequired = true
                  )
                }

                override def property(
                    name: String,
                    handler: JsonFormatVisitable,
                    propertyTypeHint: JavaType
                ): Unit = {
                  l(
                    s"JsonObjectFormatVisitor.property: name:${name} handler:${handler} propertyTypeHint:${propertyTypeHint}"
                  )
                  myPropertyHandler(name, propertyTypeHint, None, jsonPropertyRequired = true)
                }

                // Checks to see if a javax.validation field that makes our field required is present.
                private def validationAnnotationRequired(prop: Option[BeanProperty]): Boolean = {
                  prop.exists(p =>
                    selectAnnotation(p, classOf[NotNull]).isDefined || selectAnnotation(
                      p,
                      classOf[NotBlank]
                    ).isDefined || selectAnnotation(p, classOf[NotEmpty]).isDefined
                  )
                }
              })
            } else None
        }

        if (level == 0) {
          // This is the first level - we must not use definitions
          objectBuilder(node).orNull
        } else {
          val definitionInfo: DefinitionInfo =
            definitionsHandler.getOrCreateDefinition(_type)(objectBuilder)

          definitionInfo.ref.foreach { r =>
            // Must add ref to def at "this location"
            node.put("$ref", r)
          }

          definitionInfo.jsonObjectFormatVisitor.orNull
        }

      }

    }

  }

  private def extractMinimalClassnameId(baseType: JavaType, child: JavaType) = {
    // code taken straight from Jackson's MinimalClassNameIdResolver
    val base = baseType.getRawClass.getName
    val ix = base.lastIndexOf('.')
    val _basePackagePrefix = if (ix < 0) { // can this ever occur?
      "."
    } else {
      base.substring(0, ix + 1)
    }
    val n = child.getRawClass.getName
    if (n.startsWith(_basePackagePrefix)) { // note: we will leave the leading dot in there
      n.substring(_basePackagePrefix.length - 1)
    } else {
      n
    }
  }

  private def merge(mainNode: JsonNode, updateNode: JsonNode): Unit = {
    val fieldNames = updateNode.fieldNames()
    while (fieldNames.hasNext) {

      val fieldName = fieldNames.next()
      val jsonNode = mainNode.get(fieldName)
      // if field exists and is an embedded object
      if (jsonNode != null && jsonNode.isObject) {
        merge(jsonNode, updateNode.get(fieldName))
      } else {
        mainNode match {
          case node: ObjectNode =>
            // Overwrite field
            val value = updateNode.get(fieldName)
            node.set(fieldName, value)
            ()
          case _ =>
        }
      }

    }
  }

  def generateTitleFromPropertyName(propertyName: String): String = {
    // Code found here: http://stackoverflow.com/questions/2559759/how-do-i-convert-camelcase-into-human-readable-names-in-java
    val s = propertyName.replaceAll(
      String.format(
        "%s|%s|%s",
        "(?<=[A-Z])(?=[A-Z][a-z])",
        "(?<=[^A-Z])(?=[A-Z])",
        "(?<=[A-Za-z])(?=[^A-Za-z])"
      ),
      " "
    )

    // Make the first letter uppercase
    s.substring(0, 1).toUpperCase() + s.substring(1)
  }

  def resolvePropertyFormat(_type: JavaType, objectMapper: ObjectMapper): Option[String] = {
    val ac = AnnotatedClassResolver.resolve(
      objectMapper.getDeserializationConfig,
      _type,
      objectMapper.getDeserializationConfig
    )
    resolvePropertyFormat(
      Option(ac.getAnnotation(classOf[JsonSchemaFormat])),
      _type.getRawClass.getName
    )
  }

  def resolvePropertyFormat(prop: BeanProperty): Option[String] = {
    // Prefer format specified in annotation
    resolvePropertyFormat(
      Option(prop.getAnnotation(classOf[JsonSchemaFormat])),
      prop.getType.getRawClass.getName
    )
  }

  def resolvePropertyFormat(
      jsonSchemaFormatAnnotation: Option[JsonSchemaFormat],
      rawClassName: String
  ): Option[String] = {
    // Prefer format specified in annotation
    jsonSchemaFormatAnnotation
      .map { jsonSchemaFormat =>
        jsonSchemaFormat.value()
      }
      .orElse {
        config.customType2FormatMapping.get(rawClassName)
      }
  }

  def resolveType(
      propertyType: JavaType,
      prop: Option[BeanProperty],
      objectMapper: ObjectMapper
  ): JavaType = {
    val containedType = propertyType.containedType(0)

    if (containedType.getRawClass == classOf[Object]) {
      // try to resolve it via @JsonDeserialize as described here: https://github.com/FasterXML/jackson-module-scala/wiki/FAQ#deserializing-optionint-and-other-primitive-challenges
      prop
        .flatMap { p: BeanProperty =>
          Option(p.getAnnotation(classOf[JsonDeserialize]))
        }
        .flatMap { jsonDeserialize: JsonDeserialize =>
          Option(jsonDeserialize.contentAs()).map { clazz =>
            objectMapper.getTypeFactory.constructType(clazz)
          }
        }
        .getOrElse({
          if (!config.disableWarnings) {
            log.warn(
              s"$prop - Contained type is java.lang.Object and we're unable to extract its Type using fallback-approach looking for @JsonDeserialize"
            )
          }
          containedType
        })

    } else {
      // use containedType as is
      containedType
    }
  }

  def generateJsonSchema[T <: Any](clazz: Class[T]): JsonNode =
    generateJsonSchema(clazz, None, None)
  def generateJsonSchema[T <: Any](javaType: JavaType): JsonNode =
    generateJsonSchema(javaType, None, None)

  // Java-API
  def generateJsonSchema[T <: Any](clazz: Class[T], title: String, description: String): JsonNode =
    generateJsonSchema(clazz, Option(title), Option(description))
  // Java-API
  def generateJsonSchema[T <: Any](
      javaType: JavaType,
      title: String,
      description: String
  ): JsonNode = generateJsonSchema(javaType, Option(title), Option(description))

  def generateJsonSchema[T <: Any](
      clazz: Class[T],
      title: Option[String],
      description: Option[String]
  ): JsonNode = {

    def tryToReMapType(originalClass: Class[_]): Class[_] = {
      config.classTypeReMapping
        .get(originalClass)
        .map { mappedToClass: Class[_] =>
          if (debug) {
            println(s"Class $originalClass is remapped to $mappedToClass")
          }
          mappedToClass
        }
        .getOrElse(originalClass)
    }

    val clazzToUse = tryToReMapType(clazz)

    val javaType = rootObjectMapper.constructType(clazzToUse)

    generateJsonSchema(javaType, title, description)

  }

  def generateJsonSchema[T <: Any](
      javaType: JavaType,
      title: Option[String],
      description: Option[String]
  ): JsonNode = {

    val rootNode = JsonNodeFactory.instance.objectNode()

    // Specify that this is a v4 json schema
    rootNode.put("$schema", config.jsonSchemaDraft.url)
    //rootNode.put("id", "http://my.site/myschema#")

    // Add schema title
    title
      .orElse {
        Some(generateTitleFromPropertyName(javaType.getRawClass.getSimpleName))
      }
      .flatMap { title =>
        // Skip it if specified to empty string
        if (title.isEmpty) None else Some(title)
      }
      .map { title =>
        rootNode.put("title", title)
      // If root class is annotated with @JsonSchemaTitle, it will later override this title
      }

    // Maybe set schema description
    description.map { d =>
      rootNode.put("description", d)
    // If root class is annotated with @JsonSchemaDescription, it will later override this description
    }

    val definitionsHandler = new DefinitionsHandler
    val rootVisitor = new MyJsonFormatVisitorWrapper(
      rootObjectMapper,
      node = rootNode,
      definitionsHandler = definitionsHandler,
      currentProperty = None
    )

    rootObjectMapper.acceptJsonFormatVisitor(javaType, rootVisitor)

    definitionsHandler.getFinalDefinitionsNode().foreach { definitionsNode =>
      rootNode.set("definitions", definitionsNode)
      ()
    }

    rootNode

  }

  implicit class JsonNodeExtension(o: JsonNode) {
    def visit(path: String, f: (ObjectNode, String) => Unit) = {
      var p = o

      val split = path.split('/')
      for (name <- split.dropRight(1)) {
        p = Option(p.get(name)).getOrElse(p.asInstanceOf[ObjectNode].putObject(name))
      }
      f(p.asInstanceOf[ObjectNode], split.last)
    }
  }
}
