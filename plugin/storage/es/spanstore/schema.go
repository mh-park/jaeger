package spanstore

const spanMapping = `{
   "settings":{
      "index.mapping.nested_fields.limit":50,
      "index.requests.cache.enable":true,
      "index.mapper.dynamic":false,
      "analysis":{
         "analyzer":{
            "traceId_analyzer":{
               "type":"custom",
               "tokenizer":"keyword",
               "filter":"traceId_filter"
            }
         },
         "filter":{
            "traceId_filter":{
               "type":"pattern_capture",
               "patterns":[
                  "([0-9a-f]{1,16})$"
               ],
               "preserve_original":true
            }
         }
      }
   },
   "mappings":{
      "_default_":{
         "_all":{
            "enabled":false
         }
      },
      "span":{
         "properties":{
            "traceID":{
               "type":"string",
               "analyzer":"traceId_analyzer",
               "fielddata":"true"
            },
            "parentSpanID":{
               "type":"keyword",
               "ignore_above":256
            },
            "spanID":{
               "type":"keyword",
               "ignore_above":256
            },
            "operationName":{
               "type":"keyword",
               "ignore_above":256
            },
            "startTime":{
               "type":"long"
            },
            "duration":{
               "type":"long"
            },
            "flags":{
               "type":"integer"
            },
            "logs":{
               "properties":{
                  "timestamp":{
                     "type":"long"
                  },
                  "tags":{
                     "type":"nested",
                     "dynamic":false,
                     "properties":{
                        "key":{
                           "type":"keyword",
                           "ignore_above":256
                        },
                        "value":{
                           "type":"keyword",
                           "ignore_above":256
                        },
                        "tagType":{
                           "type":"keyword",
                           "ignore_above":256
                        }
                     }
                  }
               }
            },
            "process":{
               "properties":{
                  "serviceName":{
                     "type":"keyword",
                     "ignore_above":256
                  },
                  "tags":{
                     "type":"nested",
                     "dynamic":false,
                     "properties":{
                        "key":{
                           "type":"keyword",
                           "ignore_above":256
                        },
                        "value":{
                           "type":"keyword",
                           "ignore_above":256
                        },
                        "tagType":{
                           "type":"keyword",
                           "ignore_above":256
                        }
                     }
                  }
               }
            },
            "references":{
               "type":"nested",
               "dynamic":false,
               "properties":{
                  "refType":{
                     "type":"keyword",
                     "ignore_above":256
                  },
                  "traceID":{
                     "type":"keyword",
                     "ignore_above":256
                  },
                  "spanID":{
                     "type":"keyword",
                     "ignore_above":256
                  }
               }
            },
            "tags":{
               "type":"nested",
               "dynamic":false,
               "properties":{
                  "key":{
                     "type":"keyword",
                     "ignore_above":256
                  },
                  "value":{
                     "type":"keyword",
                     "ignore_above":256
                  },
                  "tagType":{
                     "type":"keyword",
                     "ignore_above":256
                  }
               }
            }
         }
      },
      "service":{
         "properties":{
            "serviceName":{
               "type":"keyword",
               "ignore_above":256
            },
            "operationName":{
               "type":"keyword",
               "ignore_above":256
            }
         }
      }
   }
}`
