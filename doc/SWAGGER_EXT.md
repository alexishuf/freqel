Swagger extensions
==================

This document describes vendor-specific extensions to Swagger service 
descriptions.

Patching a swagger file
-----------------------

Provide a base swagger description using the `baseSwagger` property. The whole
swagger description may be embedded or it may be referenced through 
[JSON Reference](https://tools.ietf.org/html/draft-pbryan-zyp-json-ref-03). 
All other defined at the root element will be added to the base swagger 
document. Replacement will only occur in the case of JSON primitives. For 
arrays, elements will be added, except for elements which are objects and 
have identifying properties (e.g., parameter objects, which are 
identified by their `name`).

Example:
```yaml
baseSwagger:
  $ref: "/portal_transparencia.json"
x-paging:
  standard:
    param: pagina
    start: 1
    increment: 1
x-definitions:
  date-serializer:
    serializer: date
    date-format: "dd/MM/yyyy"
  only-numbers:
    serializer: "only-numbers"
paths:
  /api-de-dados/licitacoes:
    get:
      responses:
        200:
          x-cardinality: LOWER_BOUND(2)
      parameters:
        - name: dataInicial
          x-serializer:
            $ref: "#/x-definitions/date-serializer"
          x-path:
            path: ["dataAbertura"]
            filter:
              sparql: "FILTER($actual >= $input)"
        - name: dataFinal
          x-serializer:
            $ref: "#/x-definitions/date-serializer"
          x-path:
            path: ["dataAbertura"]
            filter:
              sparql: "FILTER($actual <= $input)"
        - codigoOrgao:
            x-serializer:
              $ref: "#/x-definitions/only-numbers"
            x-path:
              path: ["unidadeGestora", "orgaoVinculado", "codigoSIAFI"]
```  

Cardinality
-----------

Defines the cardinality of a response (or a default cardinality for all 
responses if applied to the description root).

- **Locations**: 
    - `paths/<path>/<method>/responses/<code>/`
    - `/`
- **Property**: `x-cardinality`
- **schema**: 
```json
{
  "type": "string"
}
```

Cardinalities may assume the following reliabilities, ordered from least 
reliable to most reliable:

|   Reliability   | Description                                                                                           |
|:---------------:|:------------------------------------------------------------------------------------------------------|
| LOWER_BOUND(x)  | The operation may return **more** than x elements, but it should not return **less** than x elements. | 
| UPPER_BOUND(x)  | The operation may return **less** than x elements, but it should not return **more** than x elements. |
| EXACT(x)        | The operation should return exactly x elements                                                        |

Cardinality information should only be used for query planning purposes. 
At execution time, a Web API need not abide to informed cardinality 
values nor reliability. 

Values of `x-cardinality` can be a non-negative number, in which case 
`LOWER_BOUND` reliability MUST be assumed. A reliability level may be 
explicitly set using the syntax `RELIABILITY(value)`.


Paging
------

Determines how to explore pages of responses. Useful for Web APIs that do 
not page their results using hypermedia controls.

- **Locations**: 
    - `paths/<path>/<method>/`
    - `/x-paging`
- **Property**: `x-paging`
- **Schema**:
```json
{
  "type": "object",
  "required": ["param"],
  "properties": {
      "param": {"type":  "string"},
      "start": {"type":  "number"},
      "increment": {"type":  "number"},
      "endValue": {
        "type": "object",
        "required": ["path", "value"],
        "properties": {
          "path": {"type":  "string"},
          "value": {"type":  ["null", "boolean", "number", "string"]}
        }
      } 
  }
}
``` 

If applied to the root element of the swagger description, it defines a default 
paging method. The default paging method can be overridden on a 
per-operation basis, but cannot be explicitly disabled. If a default paging 
method is defined and a given operation lacks the paging parameter, then that 
operation will not be paged.

### Sub-properties:

- `param`: `name` of the parameter (listed in the `parameters` array) that
             is used as the paging parameter. Usually this parameter will
             denote the page number or the offset into the collection of
             items in the response. **This is required**.
- `start`: Initial value of the paging parameter. **Default value is 1**. 
- `increment`: Increment of the value of the paging parameter between pages
                 **Default value is 1**.

Binding inputs and responses
----------------------------

In a query processing setting, the response of a Web API  represents a 
resource and that resource must be somehow related to parameters in order 
for the query processor to obtain values for tha parameters either from a 
input query or from other services. Every `required` input parameter MUST 
either be associated to a paging strategy through `x-paging/param` or 
to a path through `x-path` defined below.

- **Location**: `paths/<path>/<method>/parameters/`
- **Property**: `x-path`
- **Schema**: 
```json
{
  "type": "object",
  "required": ["path"],
  "properties": {
    "path": {
      "type":  "array", 
      "minItems":  1
    },
    "direction": {
      "type": "string",
      "pattern": "^(in|out)$",
      "default": "out"  
    },
    "filter": {
      "type": "string",
      "pattern": "FILTER *\\(.*\\)"
    },
    "missing": {
      "type": "boolean",
      "default": false
    }
  }
}
```

Every path is a non-empty array listing the segments (property names). If the 
direction is `in`, the path starts at the parameter and arrives at the 
response resource. If the direction is `out`, the path starts at the response
resource and arrives at the parameter. In the case of `out` paths, the path 
need not exist in the response. 

### Other attributes of x-path

`filter`: When present, the semantic is that the parameter corresponds to a 
filter over the values at the mapped path. The value is an object where 
property names indicate the filter language and the values, the filter 
expressions in that language. All filters in the different languages are 
considered equivalent. Currently, only SPARQL filters are allowed, with the 
filter expressions corresponding to the `FILTER`  clause (including the 
`FILTER` keyword). Allowed variables in the filter are `$input` corresponding 
to the value given as input to the parameter and `$actual`, corresponding to 
the value in the `path` associated to the parameter.
**If the value of `filter` is an array**, then all filter objects behave as 
if under conjunction: all must hold.  

Example:

```json
{
  "name": "dataInicial",
  "x-path": {
    "path": ["dataAbertura"],
    "direction": "out",
    "filter": {
      "sparql": "FILTER($actual >= $input)"
    } 
  }
}
```

`index` (used within `filter`): If given, the value that binds to the `$input` 
variable in the filter is the `index-th` value in the array that is the value 
of the API parameter. If `na-value: null` in the API parameter, there must be 
filters with an `index` for every slot in the array.

`required` (used within `filter`): If given, the filter must be matched to a 
SPARQL FILTER() during matching, else the Web API endpoint cannot be invoked. 
The default value is true, since this appears to eb the most common scenario. 
If a swagger parameter has all its `filter`s with `required: false`, then it 
is possible to match a literal value (or a resource) to the Swagger parameter 
as well as matching FILTER()s. Note that in an actual query You will either 
match a value or match FILTER()s, matching both is nonsense.  

`missing`: When present, this informs that the actual value mapped by `path` 
will not be present in responses, even if it is present in the response schema.
(When a path to which a parameter is mapped is missing the schema, tha value
 also is not expected to appear in responses.). values are booleans and if 
 not present, false is assumed.
 
`na-value`: If some `filter`s use the `index` attribute, it is possible 
that only some filters match leaving some values in the array undefined. 
Those slots of the array are filled with this value. The default is an empty 
string. Setting this to `null` disallows NA value insertion and thus, all 
slots of the array must receive bound values through `filter` matches.

Controlling serialization of parameters
---------------------------------------

Each parameter may be annotated with serializers that control how values will 
be serialized to strings before being sent as parameters (serializers act 
before encoding rules, such as URL-encoding are applied).

- **Location**: `paths/<path>/<method>/parameters`
- **Property**: `x-serializer`
- **Schema**: See subsections

### Date

Serializes a data using the format string of a Java 
[SimpleDateFormat](https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html).

- **Schema**
```json
{
  "type": "object",
  "properties": {
    "serializer": {
      "type":  "string",
      "pattern":  "date"
    }, 
    "date-format": {
      "type": "string"
    } 
  }
}
```

### Only numbers

Strips all non-numeric characters of a value and retain only numbers, 
optionally enforcing a specific width and left-fill character.

- **Schema**:
```json
{
  "type": "object",
  "properties": {
    "serializer": {
      "type": "string",
      "pattern": "only-numbers"
    },
    "width": {
      "type": "number",
      "minExclusive" : 1
    },
    "fill": {
      "type": "string",
      "maxLength": 1,
      "minLength": 1,
      "default:": "0"
    } 
  }
}
```

Two properties may be defined: a fixed width of the output string, `width` 
and a `fill` character. If width is not given, `fill` has no effect and the 
serializer merely strips non-numeric characters. If `width` is given, 
it must be at least 1 and and when the input has less than `width` numbers, 
the `fill` character will be inserted on left side repeatedly until the 
resulting string has with characters. The default value for fill is `0`.

Controlling parsing of JSON elements
------------------------------------  

Some JSON elements, specially those that contain strings require further 
parsing before being transformed into RDF literals. These extensions define 
how such parsing is done. All extensions can be attached to the root (meaning 
they will apply to all json elements that the schema declares being of the 
relevant format -- e.g., `date`) or the schema elements themselves. 

### Date

Interprets the string as a date formated using the 
[SimpleDateFormat](https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html)
format string.

- **Location**: Swagger root or JSON schema element (alongside `format`)
- **Property**: `x-parser`
- **Schema**:
```json
{
  "type": "object",
  "properties": {
    "parser": {
      "type": "string",
      "pattern": "date"
    },
    "date-format": {
      "type": "string"
    }
  }
}
```

Configuring a ResponseParser
----------------------------

While the previous section discussed controlling parsing of individual 
JSON values into RDF, it may be necessary to configure the overall parser 
of the response into RDF. To configure response parsers add an entry under 
`x-response-parser`either at the swagger root (to apply that response parser 
definition globally or at a specific method of a swagger path). Response 
parsers defined at the path level override globally defined response parsers. 
Within a list of path-specific or global response parsers, the first entry 
whose media-type matches the response media type will be chosen.

- **Location**: Swagger root or method of swagger path (alongside `produces`)
- **Property**: `x-response-parser`
- **Schema**:
```json
{
  "type": "object",
  "properties": {
    "media-type": {
      "type": "string",
    },
    "response-parser": {
      "type": "string"
    }
  }
}
```

Additional properties are processed according to the response parser.

### mapped-json Response parser
- Name (with `response-parser`): `mapped-json`
- Properties:
  - `context`: a key-value map where keys are JSON properties and the 
     value is the URIs of the RDF predicate (as a string) that should be 
     used to represent the JSON property.
  - `prefix`: Use this a an URI prefix to generate predicate URIs for any 
    JSON property that is not defined in `context`.

Example: 
```json
{
  "x-response-parser": [
    {
      "media-type": "*/*",
      "response-parser": "mapped-json",
      "context": {
        "mbox": "http://xmlns.com/foaf/0.1/mbox"
      },
      "prefix": "http://example.org/"
    }
  ]
}
``` 
