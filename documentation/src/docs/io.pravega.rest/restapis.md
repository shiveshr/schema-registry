# Pravega Schema Registry APIs


<a name="overview"></a>
## Overview
REST APIs for Pravega Schema Registry.


### Version information
*Version* : 0.0.1


### License information
*License* : Apache 2.0  
*License URL* : http://www.apache.org/licenses/LICENSE-2.0  
*Terms of service* : null


### URI scheme
*BasePath* : /v1  
*Schemes* : HTTP


### Tags

* Group : Group related APIs
* Schemas : Schema related APIs




<a name="paths"></a>
## Paths

<a name="creategroup"></a>
### POST /groups

#### Description
Create a new Group


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Body**|**CreateGroupRequest**  <br>*required*|The Group configuration|[CreateGroupRequest](#creategroup-creategrouprequest)|

<a name="creategroup-creategrouprequest"></a>
**CreateGroupRequest**

|Name|Description|Schema|
|---|---|---|
|**groupName**  <br>*required*|**Example** : `"string"`|string|
|**groupProperties**  <br>*required*|**Example** : `"[groupproperties](#groupproperties)"`|[GroupProperties](#groupproperties)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**201**|Successfully added group|No Content|
|**409**|Group with given name already exists|No Content|
|**500**|Internal server error while creating a Group|No Content|


#### Consumes

* `application/json`


#### Tags

* Group


#### Example HTTP request

##### Request path
```
/groups
```


##### Request body
```json
{
  "groupName" : "string",
  "groupProperties" : {
    "serializationFormat" : {
      "serializationFormat" : "string",
      "customTypeName" : "string"
    },
    "schemaValidationRules" : {
      "rules" : {
        "string" : "[schemavalidationrule](#schemavalidationrule)"
      }
    },
    "allowMultipleTypes" : true,
    "properties" : {
      "string" : "string"
    }
  }
}
```


<a name="listgroups"></a>
### GET /groups

#### Description
List all groups


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Query**|**continuationToken**  <br>*optional*|Continuation token|string|
|**Query**|**limit**  <br>*optional*|The numbers of items to return|integer|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|List of all groups|[ListGroupsResponse](#listgroupsresponse)|
|**500**|Internal server error while fetching the list of Groups|No Content|


#### Produces

* `application/json`


#### Tags

* Group


#### Example HTTP request

##### Request path
```
/groups
```


#### Example HTTP response

##### Response 200
```json
{
  "groups" : {
    "string" : "[groupproperties](#groupproperties)"
  },
  "continuationToken" : "string"
}
```


<a name="getgroupproperties"></a>
### GET /groups/{groupName}

#### Description
Fetch the properties of an existing Group


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**groupName**  <br>*required*|Group name|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Found Group properties|[GroupProperties](#groupproperties)|
|**404**|Group with given name not found|No Content|
|**500**|Internal server error while fetching Group details|No Content|


#### Produces

* `application/json`


#### Tags

* Group


#### Example HTTP request

##### Request path
```
/groups/string
```


#### Example HTTP response

##### Response 200
```json
{
  "serializationFormat" : {
    "serializationFormat" : "string",
    "customTypeName" : "string"
  },
  "schemaValidationRules" : {
    "rules" : {
      "string" : "[schemavalidationrule](#schemavalidationrule)"
    }
  },
  "allowMultipleTypes" : true,
  "properties" : {
    "string" : "string"
  }
}
```


<a name="deletegroup"></a>
### DELETE /groups/{groupName}

#### Description
Delete a Group


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**groupName**  <br>*required*|Group name|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**204**|Successfully deleted the Group|No Content|
|**500**|Internal server error while deleting the Group|No Content|


#### Tags

* Group


#### Example HTTP request

##### Request path
```
/groups/string
```


<a name="addcodectype"></a>
### POST /groups/{groupName}/codecTypes

#### Description
Adds a new codecType to the group.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**groupName**  <br>*required*|Group name|string|
|**Body**|**codecType**  <br>*required*|The codecType|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**201**|Successfully added codecType to group|No Content|
|**404**|Group not found|No Content|
|**500**|Internal server error while registering codectype to a Group|No Content|


#### Consumes

* `application/json`


#### Tags

* Group


#### Example HTTP request

##### Request path
```
/groups/string/codecTypes
```


##### Request body
```json
{ }
```


<a name="getcodectypeslist"></a>
### GET /groups/{groupName}/codecTypes

#### Description
Get codecTypes for the group.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**groupName**  <br>*required*|Group name|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Found CodecTypes|[CodecTypesList](#codectypeslist)|
|**404**|Group or encoding id with given name not found|No Content|
|**500**|Internal server error while fetching codecTypes registered|No Content|


#### Produces

* `application/json`


#### Tags

* Group


#### Example HTTP request

##### Request path
```
/groups/string/codecTypes
```


#### Example HTTP response

##### Response 200
```json
{
  "codecTypes" : [ "string" ]
}
```


<a name="getencodingid"></a>
### PUT /groups/{groupName}/encodings

#### Description
Get an encoding id that uniquely identifies a schema version and codec type pair.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**groupName**  <br>*required*|Group name|string|
|**Body**|**GetEncodingIdRequest**  <br>*required*|Get schema corresponding to the version|[GetEncodingIdRequest](#getencodingid-getencodingidrequest)|

<a name="getencodingid-getencodingidrequest"></a>
**GetEncodingIdRequest**

|Name|Description|Schema|
|---|---|---|
|**codecType**  <br>*required*|**Example** : `"string"`|string|
|**versionInfo**  <br>*required*|**Example** : `"[versioninfo](#versioninfo)"`|[VersionInfo](#versioninfo)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Found Encoding|[EncodingId](#encodingid)|
|**404**|Group with given name or version not found|No Content|
|**412**|Codec type not registered|No Content|
|**500**|Internal server error while getting encoding id|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Group


#### Example HTTP request

##### Request path
```
/groups/string/encodings
```


##### Request body
```json
{
  "versionInfo" : {
    "type" : "string",
    "version" : 0,
    "ordinal" : 0
  },
  "codecType" : "string"
}
```


#### Example HTTP response

##### Response 200
```json
{
  "encodingId" : 0
}
```


<a name="getencodinginfo"></a>
### GET /groups/{groupName}/encodings/{encodingId}

#### Description
Get the encoding information corresponding to the encoding id.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**encodingId**  <br>*required*|Encoding id that identifies a unique combination of schema and codec type|integer (int32)|
|**Path**|**groupName**  <br>*required*|Group name|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Found Encoding|[EncodingInfo](#encodinginfo)|
|**404**|Group or encoding id with given name not found|No Content|
|**500**|Internal server error while getting encoding info corresponding to encoding id|No Content|


#### Produces

* `application/json`


#### Tags

* Group


#### Example HTTP request

##### Request path
```
/groups/string/encodings/0
```


#### Example HTTP response

##### Response 200
```json
{
  "schemaInfo" : {
    "type" : "string",
    "serializationFormat" : {
      "serializationFormat" : "string",
      "customTypeName" : "string"
    },
    "schemaData" : "string",
    "properties" : {
      "string" : "string"
    }
  },
  "versionInfo" : {
    "type" : "string",
    "version" : 0,
    "ordinal" : 0
  },
  "codecType" : "string"
}
```


<a name="getgrouphistory"></a>
### GET /groups/{groupName}/history

#### Description
Fetch the history of schema evolution of a Group


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**groupName**  <br>*required*|Group name|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Found Group history|[GroupHistory](#grouphistory)|
|**404**|Group with given name not found|No Content|
|**500**|Internal server error while fetching Group history|No Content|


#### Produces

* `application/json`


#### Tags

* Group


#### Example HTTP request

##### Request path
```
/groups/string/history
```


#### Example HTTP response

##### Response 200
```json
{
  "history" : [ {
    "schemaInfo" : {
      "type" : "string",
      "serializationFormat" : {
        "serializationFormat" : "string",
        "customTypeName" : "string"
      },
      "schemaData" : "string",
      "properties" : {
        "string" : "string"
      }
    },
    "version" : {
      "type" : "string",
      "version" : 0,
      "ordinal" : 0
    },
    "validationRules" : {
      "rules" : {
        "string" : "[schemavalidationrule](#schemavalidationrule)"
      }
    },
    "timestamp" : 0,
    "schemaString" : "string"
  } ]
}
```


<a name="updateschemavalidationrules"></a>
### PUT /groups/{groupName}/rules

#### Description
update schema validation rules of an existing Group


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**groupName**  <br>*required*|Group name|string|
|**Body**|**UpdateValidationRulesRequest**  <br>*required*|update group policy|[UpdateValidationRulesRequest](#updateschemavalidationrules-updatevalidationrulesrequest)|

<a name="updateschemavalidationrules-updatevalidationrulesrequest"></a>
**UpdateValidationRulesRequest**

|Name|Description|Schema|
|---|---|---|
|**previousRules**  <br>*optional*|**Example** : `"[schemavalidationrules](#schemavalidationrules)"`|[SchemaValidationRules](#schemavalidationrules)|
|**validationRules**  <br>*required*|**Example** : `"[schemavalidationrules](#schemavalidationrules)"`|[SchemaValidationRules](#schemavalidationrules)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Updated schema validation policy|No Content|
|**404**|Group with given name not found|No Content|
|**409**|Write conflict|No Content|
|**500**|Internal server error while updating Group's schema validation rules|No Content|


#### Consumes

* `application/json`


#### Tags

* Group


#### Example HTTP request

##### Request path
```
/groups/string/rules
```


##### Request body
```json
{
  "validationRules" : {
    "rules" : {
      "string" : "[schemavalidationrule](#schemavalidationrule)"
    }
  },
  "previousRules" : {
    "rules" : {
      "string" : "[schemavalidationrule](#schemavalidationrule)"
    }
  }
}
```


<a name="getschemas"></a>
### GET /groups/{groupName}/schemas

#### Description
Fetch latest schema versions for all objects identified by SchemaInfo#type under a Group. If query param type is specified then latest schema for the type is returned.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**groupName**  <br>*required*|Group name|string|
|**Query**|**type**  <br>*optional*|Type of object|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Latest schemas for all objects identified by SchemaInfo#type under the group|[SchemaVersionsList](#schemaversionslist)|
|**404**|Group with given name not found|No Content|
|**500**|Internal server error while fetching Group's latest schemas|No Content|


#### Produces

* `application/json`


#### Tags

* Group


#### Example HTTP request

##### Request path
```
/groups/string/schemas
```


#### Example HTTP response

##### Response 200
```json
{
  "schemas" : [ {
    "schemaInfo" : {
      "type" : "string",
      "serializationFormat" : {
        "serializationFormat" : "string",
        "customTypeName" : "string"
      },
      "schemaData" : "string",
      "properties" : {
        "string" : "string"
      }
    },
    "version" : {
      "type" : "string",
      "version" : 0,
      "ordinal" : 0
    }
  } ]
}
```


<a name="addschema"></a>
### POST /groups/{groupName}/schemas/versions

#### Description
Adds a new schema to the group


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**groupName**  <br>*required*|Group name|string|
|**Body**|**schemaInfo**  <br>*required*|Add new schema to group|[SchemaInfo](#schemainfo)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**201**|Successfully added schema to the group|[VersionInfo](#versioninfo)|
|**404**|Group not found|No Content|
|**409**|Incompatible schema|No Content|
|**417**|Invalid serialization format|No Content|
|**500**|Internal server error while adding schema to group|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Group


#### Example HTTP request

##### Request path
```
/groups/string/schemas/versions
```


##### Request body
```json
{
  "type" : "string",
  "serializationFormat" : {
    "serializationFormat" : "string",
    "customTypeName" : "string"
  },
  "schemaData" : "string",
  "properties" : {
    "string" : "string"
  }
}
```


#### Example HTTP response

##### Response 201
```json
{
  "type" : "string",
  "version" : 0,
  "ordinal" : 0
}
```


<a name="getschemaversions"></a>
### GET /groups/{groupName}/schemas/versions

#### Description
Get all schema versions for the group


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**groupName**  <br>*required*|Group name|string|
|**Query**|**type**  <br>*optional*|Type of object the schema describes.|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Versioned history of schemas registered under the group|[SchemaVersionsList](#schemaversionslist)|
|**404**|Group with given name not found|No Content|
|**500**|Internal server error while fetching Group schema versions|No Content|


#### Produces

* `application/json`


#### Tags

* Group


#### Example HTTP request

##### Request path
```
/groups/string/schemas/versions
```


#### Example HTTP response

##### Response 200
```json
{
  "schemas" : [ {
    "schemaInfo" : {
      "type" : "string",
      "serializationFormat" : {
        "serializationFormat" : "string",
        "customTypeName" : "string"
      },
      "schemaData" : "string",
      "properties" : {
        "string" : "string"
      }
    },
    "version" : {
      "type" : "string",
      "version" : 0,
      "ordinal" : 0
    }
  } ]
}
```


<a name="canread"></a>
### POST /groups/{groupName}/schemas/versions/canRead

#### Description
Checks if given schema can be used for reads subject to compatibility policy in the schema validation rules.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**groupName**  <br>*required*|Group name|string|
|**Body**|**schemaInfo**  <br>*required*|Checks if schema can be used to read the data in the stream based on compatibility rules.|[SchemaInfo](#schemainfo)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Response to tell whether schema can be used to read existing schemas|[CanRead](#canread)|
|**404**|Group with given name not found|No Content|
|**500**|Internal server error while checking schema for readability|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Group


#### Example HTTP request

##### Request path
```
/groups/string/schemas/versions/canRead
```


##### Request body
```json
{
  "type" : "string",
  "serializationFormat" : {
    "serializationFormat" : "string",
    "customTypeName" : "string"
  },
  "schemaData" : "string",
  "properties" : {
    "string" : "string"
  }
}
```


#### Example HTTP response

##### Response 200
```json
{
  "compatible" : true
}
```


<a name="getschemaversion"></a>
### POST /groups/{groupName}/schemas/versions/find

#### Description
Get the version for the schema if it is registered. It does not automatically register the schema. To add new schema use addSchema


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**groupName**  <br>*required*|Group name|string|
|**Body**|**schemaInfo**  <br>*required*|Get schema corresponding to the version|[SchemaInfo](#schemainfo)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Schema version|[VersionInfo](#versioninfo)|
|**404**|Group with given name not found|No Content|
|**500**|Internal server error fetching version for schema|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Group


#### Example HTTP request

##### Request path
```
/groups/string/schemas/versions/find
```


##### Request body
```json
{
  "type" : "string",
  "serializationFormat" : {
    "serializationFormat" : "string",
    "customTypeName" : "string"
  },
  "schemaData" : "string",
  "properties" : {
    "string" : "string"
  }
}
```


#### Example HTTP response

##### Response 200
```json
{
  "type" : "string",
  "version" : 0,
  "ordinal" : 0
}
```


<a name="validate"></a>
### POST /groups/{groupName}/schemas/versions/validate

#### Description
Checks if given schema is compatible with schemas in the registry for current policy setting.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**groupName**  <br>*required*|Group name|string|
|**Body**|**ValidateRequest**  <br>*required*|Checks if schema is valid with respect to supplied validation rules|[ValidateRequest](#validate-validaterequest)|

<a name="validate-validaterequest"></a>
**ValidateRequest**

|Name|Description|Schema|
|---|---|---|
|**schemaInfo**  <br>*required*|**Example** : `"[schemainfo](#schemainfo)"`|[SchemaInfo](#schemainfo)|
|**validationRules**  <br>*optional*|**Example** : `"[schemavalidationrules](#schemavalidationrules)"`|[SchemaValidationRules](#schemavalidationrules)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Schema validation response|[Valid](#valid)|
|**404**|Group with given name not found|No Content|
|**500**|Internal server error while trying to validate schema|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Group


#### Example HTTP request

##### Request path
```
/groups/string/schemas/versions/validate
```


##### Request body
```json
{
  "schemaInfo" : {
    "type" : "string",
    "serializationFormat" : {
      "serializationFormat" : "string",
      "customTypeName" : "string"
    },
    "schemaData" : "string",
    "properties" : {
      "string" : "string"
    }
  },
  "validationRules" : {
    "rules" : {
      "string" : "[schemavalidationrule](#schemavalidationrule)"
    }
  }
}
```


#### Example HTTP response

##### Response 200
```json
{
  "valid" : true
}
```


<a name="getschemafromversionordinal"></a>
### GET /groups/{groupName}/schemas/versions/{versionOrdinal}

#### Description
Get schema from the version ordinal that uniquely identifies the schema in the group.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**groupName**  <br>*required*|Group name|string|
|**Path**|**versionOrdinal**  <br>*required*|Version ordinal|integer (int32)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Schema corresponding to the version|[SchemaInfo](#schemainfo)|
|**404**|Group with given name not found|No Content|
|**500**|Internal server error while fetching schema from version|No Content|


#### Produces

* `application/json`


#### Tags

* Group


#### Example HTTP request

##### Request path
```
/groups/string/schemas/versions/0
```


#### Example HTTP response

##### Response 200
```json
{
  "type" : "string",
  "serializationFormat" : {
    "serializationFormat" : "string",
    "customTypeName" : "string"
  },
  "schemaData" : "string",
  "properties" : {
    "string" : "string"
  }
}
```


<a name="deleteschemaversionorinal"></a>
### DELETE /groups/{groupName}/schemas/versions/{versionOrdinal}

#### Description
Delete schema identified by version from the group.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**groupName**  <br>*required*|Group name|string|
|**Path**|**versionOrdinal**  <br>*required*|Version ordinal|integer (int32)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**204**|Schema corresponding to the version|No Content|
|**404**|Group with given name not found|No Content|
|**500**|Internal server error while deleting schema from group|No Content|


#### Produces

* `application/json`


#### Tags

* Group


#### Example HTTP request

##### Request path
```
/groups/string/schemas/versions/0
```


<a name="getschemafromversion"></a>
### GET /groups/{groupName}/schemas/{type}/versions/{version}

#### Description
Get schema from the version ordinal that uniquely identifies the schema in the group.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**groupName**  <br>*required*|Group name|string|
|**Path**|**type**  <br>*required*|Schema type from SchemaInfo#type or VersionInfo#type|string|
|**Path**|**version**  <br>*required*|Version number|integer (int32)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Schema corresponding to the version|[SchemaInfo](#schemainfo)|
|**404**|Group with given name not found|No Content|
|**500**|Internal server error while fetching schema from version|No Content|


#### Produces

* `application/json`


#### Tags

* Group


#### Example HTTP request

##### Request path
```
/groups/string/schemas/string/versions/0
```


#### Example HTTP response

##### Response 200
```json
{
  "type" : "string",
  "serializationFormat" : {
    "serializationFormat" : "string",
    "customTypeName" : "string"
  },
  "schemaData" : "string",
  "properties" : {
    "string" : "string"
  }
}
```


<a name="deleteschemaversion"></a>
### DELETE /groups/{groupName}/schemas/{type}/versions/{version}

#### Description
Delete schema version from the group.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**groupName**  <br>*required*|Group name|string|
|**Path**|**type**  <br>*required*|Schema type from SchemaInfo#type or VersionInfo#type|string|
|**Path**|**version**  <br>*required*|Version number|integer (int32)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**204**|Schema corresponding to the version|No Content|
|**404**|Group with given name not found|No Content|
|**500**|Internal server error while deleting schema from group|No Content|


#### Produces

* `application/json`


#### Tags

* Group


#### Example HTTP request

##### Request path
```
/groups/string/schemas/string/versions/0
```


<a name="getschemareferences"></a>
### POST /schemas/addedTo

#### Description
Gets a map of groups to version info where the schema if it is registered. SchemaInfo#properties is ignored while comparing the schema.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Body**|**schemaInfo**  <br>*required*|Get schema references for the supplied schema|[SchemaInfo](#schemainfo)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Schema version|[AddedTo](#addedto)|
|**404**|Schema not found|No Content|
|**500**|Internal server error while fetching Schema references|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Schema


#### Example HTTP request

##### Request path
```
/schemas/addedTo
```


##### Request body
```json
{
  "type" : "string",
  "serializationFormat" : {
    "serializationFormat" : "string",
    "customTypeName" : "string"
  },
  "schemaData" : "string",
  "properties" : {
    "string" : "string"
  }
}
```


#### Example HTTP response

##### Response 200
```json
{
  "groups" : {
    "string" : "[versioninfo](#versioninfo)"
  }
}
```



