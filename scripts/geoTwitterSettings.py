    geoTweetsSettings = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "tweet": {
                "properties": {
                    "location": {
                        "type": "geo_point"
                    },
                    "coordinates": {
                        "properties": {
                            "coordinates": {
                                "type": "float"
                            },
                            "type": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            }
                        }
                    },
                    "created_at": {
                        "fields": {
                            "keyword": {
                                "ignore_above": 256,
                                "type": "keyword"
                            }
                        },
                        "type": "text"
                    },
                    "display_text_range": {
                        "type": "long"
                    },
                    "entities": {
                        "properties": {
                            "hashtags": {
                                "properties": {
                                    "indices": {
                                        "type": "long"
                                    },
                                    "text": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    }
                                }
                            },
                            "media": {
                                "properties": {
                                    "display_url": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "expanded_url": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "id": {
                                        "type": "long"
                                    },
                                    "id_str": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "indices": {
                                        "type": "long"
                                    },
                                    "media_url": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "media_url_https": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "sizes": {
                                        "properties": {
                                            "large": {
                                                "properties": {
                                                    "h": {
                                                        "type": "long"
                                                    },
                                                    "resize": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "w": {
                                                        "type": "long"
                                                    }
                                                }
                                            },
                                            "medium": {
                                                "properties": {
                                                    "h": {
                                                        "type": "long"
                                                    },
                                                    "resize": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "w": {
                                                        "type": "long"
                                                    }
                                                }
                                            },
                                            "small": {
                                                "properties": {
                                                    "h": {
                                                        "type": "long"
                                                    },
                                                    "resize": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "w": {
                                                        "type": "long"
                                                    }
                                                }
                                            },
                                            "thumb": {
                                                "properties": {
                                                    "h": {
                                                        "type": "long"
                                                    },
                                                    "resize": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "w": {
                                                        "type": "long"
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    "source_status_id": {
                                        "type": "long"
                                    },
                                    "source_status_id_str": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "source_user_id": {
                                        "type": "long"
                                    },
                                    "source_user_id_str": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "type": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "url": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    }
                                }
                            },
                            "urls": {
                                "properties": {
                                    "display_url": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "expanded_url": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "indices": {
                                        "type": "long"
                                    },
                                    "url": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    }
                                }
                            },
                            "user_mentions": {
                                "properties": {
                                    "id": {
                                        "type": "long"
                                    },
                                    "id_str": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "indices": {
                                        "type": "long"
                                    },
                                    "name": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "screen_name": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    }
                                }
                            }
                        }
                    },
                    "extended_entities": {
                        "properties": {
                            "media": {
                                "properties": {
                                    "display_url": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "expanded_url": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "id": {
                                        "type": "long"
                                    },
                                    "id_str": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "indices": {
                                        "type": "long"
                                    },
                                    "media_url": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "media_url_https": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "sizes": {
                                        "properties": {
                                            "large": {
                                                "properties": {
                                                    "h": {
                                                        "type": "long"
                                                    },
                                                    "resize": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "w": {
                                                        "type": "long"
                                                    }
                                                }
                                            },
                                            "medium": {
                                                "properties": {
                                                    "h": {
                                                        "type": "long"
                                                    },
                                                    "resize": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "w": {
                                                        "type": "long"
                                                    }
                                                }
                                            },
                                            "small": {
                                                "properties": {
                                                    "h": {
                                                        "type": "long"
                                                    },
                                                    "resize": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "w": {
                                                        "type": "long"
                                                    }
                                                }
                                            },
                                            "thumb": {
                                                "properties": {
                                                    "h": {
                                                        "type": "long"
                                                    },
                                                    "resize": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "w": {
                                                        "type": "long"
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    "source_status_id": {
                                        "type": "long"
                                    },
                                    "source_status_id_str": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "source_user_id": {
                                        "type": "long"
                                    },
                                    "source_user_id_str": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "type": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "url": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    }
                                }
                            }
                        }
                    },
                    "favorite_count": {
                        "type": "long"
                    },
                    "favorited": {
                        "type": "boolean"
                    },
                    "filter_level": {
                        "fields": {
                            "keyword": {
                                "ignore_above": 256,
                                "type": "keyword"
                            }
                        },
                        "type": "text"
                    },
                    "geo": {
                        "properties": {
                            "coordinates": {
                                "type": "float"
                            },
                            "type": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            }
                        }
                    },
                    "id": {
                        "type": "long"
                    },
                    "id_str": {
                        "fields": {
                            "keyword": {
                                "ignore_above": 256,
                                "type": "keyword"
                            }
                        },
                        "type": "text"
                    },
                    "in_reply_to_screen_name": {
                        "fields": {
                            "keyword": {
                                "ignore_above": 256,
                                "type": "keyword"
                            }
                        },
                        "type": "text"
                    },
                    "in_reply_to_status_id": {
                        "type": "long"
                    },
                    "in_reply_to_status_id_str": {
                        "fields": {
                            "keyword": {
                                "ignore_above": 256,
                                "type": "keyword"
                            }
                        },
                        "type": "text"
                    },
                    "in_reply_to_user_id": {
                        "type": "long"
                    },
                    "in_reply_to_user_id_str": {
                        "fields": {
                            "keyword": {
                                "ignore_above": 256,
                                "type": "keyword"
                            }
                        },
                        "type": "text"
                    },
                    "is_quote_status": {
                        "type": "boolean"
                    },
                    "lang": {
                        "fields": {
                            "keyword": {
                                "ignore_above": 256,
                                "type": "keyword"
                            }
                        },
                        "type": "text"
                    },
                    "place": {
                        "properties": {
                            "attributes": {
                                "type": "object"
                            },
                            "bounding_box": {
                                "properties": {
                                    "coordinates": {
                                        "type": "float"
                                    },
                                    "type": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    }
                                }
                            },
                            "country": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "country_code": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "full_name": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "id": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "name": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "place_type": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "url": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            }
                        }
                    },
                    "possibly_sensitive": {
                        "type": "boolean"
                    },
                    "quoted_status": {
                        "properties": {
                            "created_at": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "display_text_range": {
                                "type": "long"
                            },
                            "entities": {
                                "properties": {
                                    "urls": {
                                        "properties": {
                                            "display_url": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "expanded_url": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "indices": {
                                                "type": "long"
                                            },
                                            "url": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            }
                                        }
                                    },
                                    "user_mentions": {
                                        "properties": {
                                            "id": {
                                                "type": "long"
                                            },
                                            "id_str": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "indices": {
                                                "type": "long"
                                            },
                                            "name": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "screen_name": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            }
                                        }
                                    }
                                }
                            },
                            "extended_tweet": {
                                "properties": {
                                    "display_text_range": {
                                        "type": "long"
                                    },
                                    "entities": {
                                        "properties": {
                                            "media": {
                                                "properties": {
                                                    "display_url": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "expanded_url": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "id": {
                                                        "type": "long"
                                                    },
                                                    "id_str": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "indices": {
                                                        "type": "long"
                                                    },
                                                    "media_url": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "media_url_https": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "sizes": {
                                                        "properties": {
                                                            "large": {
                                                                "properties": {
                                                                    "h": {
                                                                        "type": "long"
                                                                    },
                                                                    "resize": {
                                                                        "fields": {
                                                                            "keyword": {
                                                                                "ignore_above": 256,
                                                                                "type": "keyword"
                                                                            }
                                                                        },
                                                                        "type": "text"
                                                                    },
                                                                    "w": {
                                                                        "type": "long"
                                                                    }
                                                                }
                                                            },
                                                            "medium": {
                                                                "properties": {
                                                                    "h": {
                                                                        "type": "long"
                                                                    },
                                                                    "resize": {
                                                                        "fields": {
                                                                            "keyword": {
                                                                                "ignore_above": 256,
                                                                                "type": "keyword"
                                                                            }
                                                                        },
                                                                        "type": "text"
                                                                    },
                                                                    "w": {
                                                                        "type": "long"
                                                                    }
                                                                }
                                                            },
                                                            "small": {
                                                                "properties": {
                                                                    "h": {
                                                                        "type": "long"
                                                                    },
                                                                    "resize": {
                                                                        "fields": {
                                                                            "keyword": {
                                                                                "ignore_above": 256,
                                                                                "type": "keyword"
                                                                            }
                                                                        },
                                                                        "type": "text"
                                                                    },
                                                                    "w": {
                                                                        "type": "long"
                                                                    }
                                                                }
                                                            },
                                                            "thumb": {
                                                                "properties": {
                                                                    "h": {
                                                                        "type": "long"
                                                                    },
                                                                    "resize": {
                                                                        "fields": {
                                                                            "keyword": {
                                                                                "ignore_above": 256,
                                                                                "type": "keyword"
                                                                            }
                                                                        },
                                                                        "type": "text"
                                                                    },
                                                                    "w": {
                                                                        "type": "long"
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    },
                                                    "type": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "url": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    }
                                                }
                                            },
                                            "urls": {
                                                "properties": {
                                                    "display_url": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "expanded_url": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "indices": {
                                                        "type": "long"
                                                    },
                                                    "url": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    "extended_entities": {
                                        "properties": {
                                            "media": {
                                                "properties": {
                                                    "display_url": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "expanded_url": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "id": {
                                                        "type": "long"
                                                    },
                                                    "id_str": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "indices": {
                                                        "type": "long"
                                                    },
                                                    "media_url": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "media_url_https": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "sizes": {
                                                        "properties": {
                                                            "large": {
                                                                "properties": {
                                                                    "h": {
                                                                        "type": "long"
                                                                    },
                                                                    "resize": {
                                                                        "fields": {
                                                                            "keyword": {
                                                                                "ignore_above": 256,
                                                                                "type": "keyword"
                                                                            }
                                                                        },
                                                                        "type": "text"
                                                                    },
                                                                    "w": {
                                                                        "type": "long"
                                                                    }
                                                                }
                                                            },
                                                            "medium": {
                                                                "properties": {
                                                                    "h": {
                                                                        "type": "long"
                                                                    },
                                                                    "resize": {
                                                                        "fields": {
                                                                            "keyword": {
                                                                                "ignore_above": 256,
                                                                                "type": "keyword"
                                                                            }
                                                                        },
                                                                        "type": "text"
                                                                    },
                                                                    "w": {
                                                                        "type": "long"
                                                                    }
                                                                }
                                                            },
                                                            "small": {
                                                                "properties": {
                                                                    "h": {
                                                                        "type": "long"
                                                                    },
                                                                    "resize": {
                                                                        "fields": {
                                                                            "keyword": {
                                                                                "ignore_above": 256,
                                                                                "type": "keyword"
                                                                            }
                                                                        },
                                                                        "type": "text"
                                                                    },
                                                                    "w": {
                                                                        "type": "long"
                                                                    }
                                                                }
                                                            },
                                                            "thumb": {
                                                                "properties": {
                                                                    "h": {
                                                                        "type": "long"
                                                                    },
                                                                    "resize": {
                                                                        "fields": {
                                                                            "keyword": {
                                                                                "ignore_above": 256,
                                                                                "type": "keyword"
                                                                            }
                                                                        },
                                                                        "type": "text"
                                                                    },
                                                                    "w": {
                                                                        "type": "long"
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    },
                                                    "type": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "url": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    "full_text": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    }
                                }
                            },
                            "favorite_count": {
                                "type": "long"
                            },
                            "favorited": {
                                "type": "boolean"
                            },
                            "filter_level": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "id": {
                                "type": "long"
                            },
                            "id_str": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "is_quote_status": {
                                "type": "boolean"
                            },
                            "lang": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "place": {
                                "properties": {
                                    "attributes": {
                                        "type": "object"
                                    },
                                    "bounding_box": {
                                        "properties": {
                                            "coordinates": {
                                                "type": "float"
                                            },
                                            "type": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            }
                                        }
                                    },
                                    "country": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "country_code": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "full_name": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "id": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "name": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "place_type": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "url": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    }
                                }
                            },
                            "possibly_sensitive": {
                                "type": "boolean"
                            },
                            "quoted_status_id": {
                                "type": "long"
                            },
                            "quoted_status_id_str": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "retweet_count": {
                                "type": "long"
                            },
                            "retweeted": {
                                "type": "boolean"
                            },
                            "source": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "text": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "truncated": {
                                "type": "boolean"
                            },
                            "user": {
                                "properties": {
                                    "contributors_enabled": {
                                        "type": "boolean"
                                    },
                                    "created_at": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "default_profile": {
                                        "type": "boolean"
                                    },
                                    "default_profile_image": {
                                        "type": "boolean"
                                    },
                                    "description": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "favourites_count": {
                                        "type": "long"
                                    },
                                    "followers_count": {
                                        "type": "long"
                                    },
                                    "friends_count": {
                                        "type": "long"
                                    },
                                    "geo_enabled": {
                                        "type": "boolean"
                                    },
                                    "id": {
                                        "type": "long"
                                    },
                                    "id_str": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "is_translator": {
                                        "type": "boolean"
                                    },
                                    "lang": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "listed_count": {
                                        "type": "long"
                                    },
                                    "location": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "name": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "profile_background_color": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "profile_background_image_url": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "profile_background_image_url_https": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "profile_background_tile": {
                                        "type": "boolean"
                                    },
                                    "profile_banner_url": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "profile_image_url": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "profile_image_url_https": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "profile_link_color": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "profile_sidebar_border_color": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "profile_sidebar_fill_color": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "profile_text_color": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "profile_use_background_image": {
                                        "type": "boolean"
                                    },
                                    "protected": {
                                        "type": "boolean"
                                    },
                                    "screen_name": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "statuses_count": {
                                        "type": "long"
                                    },
                                    "time_zone": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "url": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "utc_offset": {
                                        "type": "long"
                                    },
                                    "verified": {
                                        "type": "boolean"
                                    }
                                }
                            }
                        }
                    },
                    "quoted_status_id": {
                        "type": "long"
                    },
                    "quoted_status_id_str": {
                        "fields": {
                            "keyword": {
                                "ignore_above": 256,
                                "type": "keyword"
                            }
                        },
                        "type": "text"
                    },
                    "retweet_count": {
                        "type": "long"
                    },
                    "retweeted": {
                        "type": "boolean"
                    },
                    "retweeted_status": {
                        "properties": {
                            "created_at": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "display_text_range": {
                                "type": "long"
                            },
                            "entities": {
                                "properties": {
                                    "hashtags": {
                                        "properties": {
                                            "indices": {
                                                "type": "long"
                                            },
                                            "text": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            }
                                        }
                                    },
                                    "media": {
                                        "properties": {
                                            "display_url": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "expanded_url": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "id": {
                                                "type": "long"
                                            },
                                            "id_str": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "indices": {
                                                "type": "long"
                                            },
                                            "media_url": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "media_url_https": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "sizes": {
                                                "properties": {
                                                    "large": {
                                                        "properties": {
                                                            "h": {
                                                                "type": "long"
                                                            },
                                                            "resize": {
                                                                "fields": {
                                                                    "keyword": {
                                                                        "ignore_above": 256,
                                                                        "type": "keyword"
                                                                    }
                                                                },
                                                                "type": "text"
                                                            },
                                                            "w": {
                                                                "type": "long"
                                                            }
                                                        }
                                                    },
                                                    "medium": {
                                                        "properties": {
                                                            "h": {
                                                                "type": "long"
                                                            },
                                                            "resize": {
                                                                "fields": {
                                                                    "keyword": {
                                                                        "ignore_above": 256,
                                                                        "type": "keyword"
                                                                    }
                                                                },
                                                                "type": "text"
                                                            },
                                                            "w": {
                                                                "type": "long"
                                                            }
                                                        }
                                                    },
                                                    "small": {
                                                        "properties": {
                                                            "h": {
                                                                "type": "long"
                                                            },
                                                            "resize": {
                                                                "fields": {
                                                                    "keyword": {
                                                                        "ignore_above": 256,
                                                                        "type": "keyword"
                                                                    }
                                                                },
                                                                "type": "text"
                                                            },
                                                            "w": {
                                                                "type": "long"
                                                            }
                                                        }
                                                    },
                                                    "thumb": {
                                                        "properties": {
                                                            "h": {
                                                                "type": "long"
                                                            },
                                                            "resize": {
                                                                "fields": {
                                                                    "keyword": {
                                                                        "ignore_above": 256,
                                                                        "type": "keyword"
                                                                    }
                                                                },
                                                                "type": "text"
                                                            },
                                                            "w": {
                                                                "type": "long"
                                                            }
                                                        }
                                                    }
                                                }
                                            },
                                            "source_status_id": {
                                                "type": "long"
                                            },
                                            "source_status_id_str": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "source_user_id": {
                                                "type": "long"
                                            },
                                            "source_user_id_str": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "type": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "url": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            }
                                        }
                                    },
                                    "urls": {
                                        "properties": {
                                            "display_url": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "expanded_url": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "indices": {
                                                "type": "long"
                                            },
                                            "url": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            }
                                        }
                                    },
                                    "user_mentions": {
                                        "properties": {
                                            "id": {
                                                "type": "long"
                                            },
                                            "id_str": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "indices": {
                                                "type": "long"
                                            },
                                            "name": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "screen_name": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            }
                                        }
                                    }
                                }
                            },
                            "extended_entities": {
                                "properties": {
                                    "media": {
                                        "properties": {
                                            "display_url": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "expanded_url": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "id": {
                                                "type": "long"
                                            },
                                            "id_str": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "indices": {
                                                "type": "long"
                                            },
                                            "media_url": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "media_url_https": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "sizes": {
                                                "properties": {
                                                    "large": {
                                                        "properties": {
                                                            "h": {
                                                                "type": "long"
                                                            },
                                                            "resize": {
                                                                "fields": {
                                                                    "keyword": {
                                                                        "ignore_above": 256,
                                                                        "type": "keyword"
                                                                    }
                                                                },
                                                                "type": "text"
                                                            },
                                                            "w": {
                                                                "type": "long"
                                                            }
                                                        }
                                                    },
                                                    "medium": {
                                                        "properties": {
                                                            "h": {
                                                                "type": "long"
                                                            },
                                                            "resize": {
                                                                "fields": {
                                                                    "keyword": {
                                                                        "ignore_above": 256,
                                                                        "type": "keyword"
                                                                    }
                                                                },
                                                                "type": "text"
                                                            },
                                                            "w": {
                                                                "type": "long"
                                                            }
                                                        }
                                                    },
                                                    "small": {
                                                        "properties": {
                                                            "h": {
                                                                "type": "long"
                                                            },
                                                            "resize": {
                                                                "fields": {
                                                                    "keyword": {
                                                                        "ignore_above": 256,
                                                                        "type": "keyword"
                                                                    }
                                                                },
                                                                "type": "text"
                                                            },
                                                            "w": {
                                                                "type": "long"
                                                            }
                                                        }
                                                    },
                                                    "thumb": {
                                                        "properties": {
                                                            "h": {
                                                                "type": "long"
                                                            },
                                                            "resize": {
                                                                "fields": {
                                                                    "keyword": {
                                                                        "ignore_above": 256,
                                                                        "type": "keyword"
                                                                    }
                                                                },
                                                                "type": "text"
                                                            },
                                                            "w": {
                                                                "type": "long"
                                                            }
                                                        }
                                                    }
                                                }
                                            },
                                            "source_status_id": {
                                                "type": "long"
                                            },
                                            "source_status_id_str": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "source_user_id": {
                                                "type": "long"
                                            },
                                            "source_user_id_str": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "type": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "url": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "video_info": {
                                                "properties": {
                                                    "aspect_ratio": {
                                                        "type": "long"
                                                    },
                                                    "duration_millis": {
                                                        "type": "long"
                                                    },
                                                    "variants": {
                                                        "properties": {
                                                            "bitrate": {
                                                                "type": "long"
                                                            },
                                                            "content_type": {
                                                                "fields": {
                                                                    "keyword": {
                                                                        "ignore_above": 256,
                                                                        "type": "keyword"
                                                                    }
                                                                },
                                                                "type": "text"
                                                            },
                                                            "url": {
                                                                "fields": {
                                                                    "keyword": {
                                                                        "ignore_above": 256,
                                                                        "type": "keyword"
                                                                    }
                                                                },
                                                                "type": "text"
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            },
                            "extended_tweet": {
                                "properties": {
                                    "display_text_range": {
                                        "type": "long"
                                    },
                                    "entities": {
                                        "properties": {
                                            "hashtags": {
                                                "properties": {
                                                    "indices": {
                                                        "type": "long"
                                                    },
                                                    "text": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    }
                                                }
                                            },
                                            "media": {
                                                "properties": {
                                                    "display_url": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "expanded_url": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "id": {
                                                        "type": "long"
                                                    },
                                                    "id_str": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "indices": {
                                                        "type": "long"
                                                    },
                                                    "media_url": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "media_url_https": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "sizes": {
                                                        "properties": {
                                                            "large": {
                                                                "properties": {
                                                                    "h": {
                                                                        "type": "long"
                                                                    },
                                                                    "resize": {
                                                                        "fields": {
                                                                            "keyword": {
                                                                                "ignore_above": 256,
                                                                                "type": "keyword"
                                                                            }
                                                                        },
                                                                        "type": "text"
                                                                    },
                                                                    "w": {
                                                                        "type": "long"
                                                                    }
                                                                }
                                                            },
                                                            "medium": {
                                                                "properties": {
                                                                    "h": {
                                                                        "type": "long"
                                                                    },
                                                                    "resize": {
                                                                        "fields": {
                                                                            "keyword": {
                                                                                "ignore_above": 256,
                                                                                "type": "keyword"
                                                                            }
                                                                        },
                                                                        "type": "text"
                                                                    },
                                                                    "w": {
                                                                        "type": "long"
                                                                    }
                                                                }
                                                            },
                                                            "small": {
                                                                "properties": {
                                                                    "h": {
                                                                        "type": "long"
                                                                    },
                                                                    "resize": {
                                                                        "fields": {
                                                                            "keyword": {
                                                                                "ignore_above": 256,
                                                                                "type": "keyword"
                                                                            }
                                                                        },
                                                                        "type": "text"
                                                                    },
                                                                    "w": {
                                                                        "type": "long"
                                                                    }
                                                                }
                                                            },
                                                            "thumb": {
                                                                "properties": {
                                                                    "h": {
                                                                        "type": "long"
                                                                    },
                                                                    "resize": {
                                                                        "fields": {
                                                                            "keyword": {
                                                                                "ignore_above": 256,
                                                                                "type": "keyword"
                                                                            }
                                                                        },
                                                                        "type": "text"
                                                                    },
                                                                    "w": {
                                                                        "type": "long"
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    },
                                                    "type": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "url": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "video_info": {
                                                        "properties": {
                                                            "aspect_ratio": {
                                                                "type": "long"
                                                            },
                                                            "duration_millis": {
                                                                "type": "long"
                                                            },
                                                            "variants": {
                                                                "properties": {
                                                                    "bitrate": {
                                                                        "type": "long"
                                                                    },
                                                                    "content_type": {
                                                                        "fields": {
                                                                            "keyword": {
                                                                                "ignore_above": 256,
                                                                                "type": "keyword"
                                                                            }
                                                                        },
                                                                        "type": "text"
                                                                    },
                                                                    "url": {
                                                                        "fields": {
                                                                            "keyword": {
                                                                                "ignore_above": 256,
                                                                                "type": "keyword"
                                                                            }
                                                                        },
                                                                        "type": "text"
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            },
                                            "urls": {
                                                "properties": {
                                                    "display_url": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "expanded_url": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "indices": {
                                                        "type": "long"
                                                    },
                                                    "url": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    }
                                                }
                                            },
                                            "user_mentions": {
                                                "properties": {
                                                    "id": {
                                                        "type": "long"
                                                    },
                                                    "id_str": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "indices": {
                                                        "type": "long"
                                                    },
                                                    "name": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "screen_name": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    "extended_entities": {
                                        "properties": {
                                            "media": {
                                                "properties": {
                                                    "display_url": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "expanded_url": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "id": {
                                                        "type": "long"
                                                    },
                                                    "id_str": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "indices": {
                                                        "type": "long"
                                                    },
                                                    "media_url": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "media_url_https": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "sizes": {
                                                        "properties": {
                                                            "large": {
                                                                "properties": {
                                                                    "h": {
                                                                        "type": "long"
                                                                    },
                                                                    "resize": {
                                                                        "fields": {
                                                                            "keyword": {
                                                                                "ignore_above": 256,
                                                                                "type": "keyword"
                                                                            }
                                                                        },
                                                                        "type": "text"
                                                                    },
                                                                    "w": {
                                                                        "type": "long"
                                                                    }
                                                                }
                                                            },
                                                            "medium": {
                                                                "properties": {
                                                                    "h": {
                                                                        "type": "long"
                                                                    },
                                                                    "resize": {
                                                                        "fields": {
                                                                            "keyword": {
                                                                                "ignore_above": 256,
                                                                                "type": "keyword"
                                                                            }
                                                                        },
                                                                        "type": "text"
                                                                    },
                                                                    "w": {
                                                                        "type": "long"
                                                                    }
                                                                }
                                                            },
                                                            "small": {
                                                                "properties": {
                                                                    "h": {
                                                                        "type": "long"
                                                                    },
                                                                    "resize": {
                                                                        "fields": {
                                                                            "keyword": {
                                                                                "ignore_above": 256,
                                                                                "type": "keyword"
                                                                            }
                                                                        },
                                                                        "type": "text"
                                                                    },
                                                                    "w": {
                                                                        "type": "long"
                                                                    }
                                                                }
                                                            },
                                                            "thumb": {
                                                                "properties": {
                                                                    "h": {
                                                                        "type": "long"
                                                                    },
                                                                    "resize": {
                                                                        "fields": {
                                                                            "keyword": {
                                                                                "ignore_above": 256,
                                                                                "type": "keyword"
                                                                            }
                                                                        },
                                                                        "type": "text"
                                                                    },
                                                                    "w": {
                                                                        "type": "long"
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    },
                                                    "type": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "url": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "video_info": {
                                                        "properties": {
                                                            "aspect_ratio": {
                                                                "type": "long"
                                                            },
                                                            "duration_millis": {
                                                                "type": "long"
                                                            },
                                                            "variants": {
                                                                "properties": {
                                                                    "bitrate": {
                                                                        "type": "long"
                                                                    },
                                                                    "content_type": {
                                                                        "fields": {
                                                                            "keyword": {
                                                                                "ignore_above": 256,
                                                                                "type": "keyword"
                                                                            }
                                                                        },
                                                                        "type": "text"
                                                                    },
                                                                    "url": {
                                                                        "fields": {
                                                                            "keyword": {
                                                                                "ignore_above": 256,
                                                                                "type": "keyword"
                                                                            }
                                                                        },
                                                                        "type": "text"
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    "full_text": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    }
                                }
                            },
                            "favorite_count": {
                                "type": "long"
                            },
                            "favorited": {
                                "type": "boolean"
                            },
                            "filter_level": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "id": {
                                "type": "long"
                            },
                            "id_str": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "is_quote_status": {
                                "type": "boolean"
                            },
                            "lang": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "place": {
                                "properties": {
                                    "attributes": {
                                        "type": "object"
                                    },
                                    "bounding_box": {
                                        "properties": {
                                            "coordinates": {
                                                "type": "float"
                                            },
                                            "type": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            }
                                        }
                                    },
                                    "country": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "country_code": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "full_name": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "id": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "name": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "place_type": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "url": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    }
                                }
                            },
                            "possibly_sensitive": {
                                "type": "boolean"
                            },
                            "quoted_status": {
                                "properties": {
                                    "created_at": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "display_text_range": {
                                        "type": "long"
                                    },
                                    "entities": {
                                        "properties": {
                                            "urls": {
                                                "properties": {
                                                    "display_url": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "expanded_url": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "indices": {
                                                        "type": "long"
                                                    },
                                                    "url": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    }
                                                }
                                            },
                                            "user_mentions": {
                                                "properties": {
                                                    "id": {
                                                        "type": "long"
                                                    },
                                                    "id_str": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "indices": {
                                                        "type": "long"
                                                    },
                                                    "name": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    },
                                                    "screen_name": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    }
                                                }
                                            }
                                        }
                                    },
                                    "extended_tweet": {
                                        "properties": {
                                            "display_text_range": {
                                                "type": "long"
                                            },
                                            "entities": {
                                                "properties": {
                                                    "media": {
                                                        "properties": {
                                                            "display_url": {
                                                                "fields": {
                                                                    "keyword": {
                                                                        "ignore_above": 256,
                                                                        "type": "keyword"
                                                                    }
                                                                },
                                                                "type": "text"
                                                            },
                                                            "expanded_url": {
                                                                "fields": {
                                                                    "keyword": {
                                                                        "ignore_above": 256,
                                                                        "type": "keyword"
                                                                    }
                                                                },
                                                                "type": "text"
                                                            },
                                                            "id": {
                                                                "type": "long"
                                                            },
                                                            "id_str": {
                                                                "fields": {
                                                                    "keyword": {
                                                                        "ignore_above": 256,
                                                                        "type": "keyword"
                                                                    }
                                                                },
                                                                "type": "text"
                                                            },
                                                            "indices": {
                                                                "type": "long"
                                                            },
                                                            "media_url": {
                                                                "fields": {
                                                                    "keyword": {
                                                                        "ignore_above": 256,
                                                                        "type": "keyword"
                                                                    }
                                                                },
                                                                "type": "text"
                                                            },
                                                            "media_url_https": {
                                                                "fields": {
                                                                    "keyword": {
                                                                        "ignore_above": 256,
                                                                        "type": "keyword"
                                                                    }
                                                                },
                                                                "type": "text"
                                                            },
                                                            "sizes": {
                                                                "properties": {
                                                                    "large": {
                                                                        "properties": {
                                                                            "h": {
                                                                                "type": "long"
                                                                            },
                                                                            "resize": {
                                                                                "fields": {
                                                                                    "keyword": {
                                                                                        "ignore_above": 256,
                                                                                        "type": "keyword"
                                                                                    }
                                                                                },
                                                                                "type": "text"
                                                                            },
                                                                            "w": {
                                                                                "type": "long"
                                                                            }
                                                                        }
                                                                    },
                                                                    "medium": {
                                                                        "properties": {
                                                                            "h": {
                                                                                "type": "long"
                                                                            },
                                                                            "resize": {
                                                                                "fields": {
                                                                                    "keyword": {
                                                                                        "ignore_above": 256,
                                                                                        "type": "keyword"
                                                                                    }
                                                                                },
                                                                                "type": "text"
                                                                            },
                                                                            "w": {
                                                                                "type": "long"
                                                                            }
                                                                        }
                                                                    },
                                                                    "small": {
                                                                        "properties": {
                                                                            "h": {
                                                                                "type": "long"
                                                                            },
                                                                            "resize": {
                                                                                "fields": {
                                                                                    "keyword": {
                                                                                        "ignore_above": 256,
                                                                                        "type": "keyword"
                                                                                    }
                                                                                },
                                                                                "type": "text"
                                                                            },
                                                                            "w": {
                                                                                "type": "long"
                                                                            }
                                                                        }
                                                                    },
                                                                    "thumb": {
                                                                        "properties": {
                                                                            "h": {
                                                                                "type": "long"
                                                                            },
                                                                            "resize": {
                                                                                "fields": {
                                                                                    "keyword": {
                                                                                        "ignore_above": 256,
                                                                                        "type": "keyword"
                                                                                    }
                                                                                },
                                                                                "type": "text"
                                                                            },
                                                                            "w": {
                                                                                "type": "long"
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            },
                                                            "type": {
                                                                "fields": {
                                                                    "keyword": {
                                                                        "ignore_above": 256,
                                                                        "type": "keyword"
                                                                    }
                                                                },
                                                                "type": "text"
                                                            },
                                                            "url": {
                                                                "fields": {
                                                                    "keyword": {
                                                                        "ignore_above": 256,
                                                                        "type": "keyword"
                                                                    }
                                                                },
                                                                "type": "text"
                                                            }
                                                        }
                                                    },
                                                    "urls": {
                                                        "properties": {
                                                            "display_url": {
                                                                "fields": {
                                                                    "keyword": {
                                                                        "ignore_above": 256,
                                                                        "type": "keyword"
                                                                    }
                                                                },
                                                                "type": "text"
                                                            },
                                                            "expanded_url": {
                                                                "fields": {
                                                                    "keyword": {
                                                                        "ignore_above": 256,
                                                                        "type": "keyword"
                                                                    }
                                                                },
                                                                "type": "text"
                                                            },
                                                            "indices": {
                                                                "type": "long"
                                                            },
                                                            "url": {
                                                                "fields": {
                                                                    "keyword": {
                                                                        "ignore_above": 256,
                                                                        "type": "keyword"
                                                                    }
                                                                },
                                                                "type": "text"
                                                            }
                                                        }
                                                    }
                                                }
                                            },
                                            "extended_entities": {
                                                "properties": {
                                                    "media": {
                                                        "properties": {
                                                            "display_url": {
                                                                "fields": {
                                                                    "keyword": {
                                                                        "ignore_above": 256,
                                                                        "type": "keyword"
                                                                    }
                                                                },
                                                                "type": "text"
                                                            },
                                                            "expanded_url": {
                                                                "fields": {
                                                                    "keyword": {
                                                                        "ignore_above": 256,
                                                                        "type": "keyword"
                                                                    }
                                                                },
                                                                "type": "text"
                                                            },
                                                            "id": {
                                                                "type": "long"
                                                            },
                                                            "id_str": {
                                                                "fields": {
                                                                    "keyword": {
                                                                        "ignore_above": 256,
                                                                        "type": "keyword"
                                                                    }
                                                                },
                                                                "type": "text"
                                                            },
                                                            "indices": {
                                                                "type": "long"
                                                            },
                                                            "media_url": {
                                                                "fields": {
                                                                    "keyword": {
                                                                        "ignore_above": 256,
                                                                        "type": "keyword"
                                                                    }
                                                                },
                                                                "type": "text"
                                                            },
                                                            "media_url_https": {
                                                                "fields": {
                                                                    "keyword": {
                                                                        "ignore_above": 256,
                                                                        "type": "keyword"
                                                                    }
                                                                },
                                                                "type": "text"
                                                            },
                                                            "sizes": {
                                                                "properties": {
                                                                    "large": {
                                                                        "properties": {
                                                                            "h": {
                                                                                "type": "long"
                                                                            },
                                                                            "resize": {
                                                                                "fields": {
                                                                                    "keyword": {
                                                                                        "ignore_above": 256,
                                                                                        "type": "keyword"
                                                                                    }
                                                                                },
                                                                                "type": "text"
                                                                            },
                                                                            "w": {
                                                                                "type": "long"
                                                                            }
                                                                        }
                                                                    },
                                                                    "medium": {
                                                                        "properties": {
                                                                            "h": {
                                                                                "type": "long"
                                                                            },
                                                                            "resize": {
                                                                                "fields": {
                                                                                    "keyword": {
                                                                                        "ignore_above": 256,
                                                                                        "type": "keyword"
                                                                                    }
                                                                                },
                                                                                "type": "text"
                                                                            },
                                                                            "w": {
                                                                                "type": "long"
                                                                            }
                                                                        }
                                                                    },
                                                                    "small": {
                                                                        "properties": {
                                                                            "h": {
                                                                                "type": "long"
                                                                            },
                                                                            "resize": {
                                                                                "fields": {
                                                                                    "keyword": {
                                                                                        "ignore_above": 256,
                                                                                        "type": "keyword"
                                                                                    }
                                                                                },
                                                                                "type": "text"
                                                                            },
                                                                            "w": {
                                                                                "type": "long"
                                                                            }
                                                                        }
                                                                    },
                                                                    "thumb": {
                                                                        "properties": {
                                                                            "h": {
                                                                                "type": "long"
                                                                            },
                                                                            "resize": {
                                                                                "fields": {
                                                                                    "keyword": {
                                                                                        "ignore_above": 256,
                                                                                        "type": "keyword"
                                                                                    }
                                                                                },
                                                                                "type": "text"
                                                                            },
                                                                            "w": {
                                                                                "type": "long"
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            },
                                                            "type": {
                                                                "fields": {
                                                                    "keyword": {
                                                                        "ignore_above": 256,
                                                                        "type": "keyword"
                                                                    }
                                                                },
                                                                "type": "text"
                                                            },
                                                            "url": {
                                                                "fields": {
                                                                    "keyword": {
                                                                        "ignore_above": 256,
                                                                        "type": "keyword"
                                                                    }
                                                                },
                                                                "type": "text"
                                                            }
                                                        }
                                                    }
                                                }
                                            },
                                            "full_text": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            }
                                        }
                                    },
                                    "favorite_count": {
                                        "type": "long"
                                    },
                                    "favorited": {
                                        "type": "boolean"
                                    },
                                    "filter_level": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "id": {
                                        "type": "long"
                                    },
                                    "id_str": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "is_quote_status": {
                                        "type": "boolean"
                                    },
                                    "lang": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "place": {
                                        "properties": {
                                            "attributes": {
                                                "type": "object"
                                            },
                                            "bounding_box": {
                                                "properties": {
                                                    "coordinates": {
                                                        "type": "float"
                                                    },
                                                    "type": {
                                                        "fields": {
                                                            "keyword": {
                                                                "ignore_above": 256,
                                                                "type": "keyword"
                                                            }
                                                        },
                                                        "type": "text"
                                                    }
                                                }
                                            },
                                            "country": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "country_code": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "full_name": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "id": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "name": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "place_type": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "url": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            }
                                        }
                                    },
                                    "possibly_sensitive": {
                                        "type": "boolean"
                                    },
                                    "quoted_status_id": {
                                        "type": "long"
                                    },
                                    "quoted_status_id_str": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "retweet_count": {
                                        "type": "long"
                                    },
                                    "retweeted": {
                                        "type": "boolean"
                                    },
                                    "source": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "text": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "truncated": {
                                        "type": "boolean"
                                    },
                                    "user": {
                                        "properties": {
                                            "contributors_enabled": {
                                                "type": "boolean"
                                            },
                                            "created_at": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "default_profile": {
                                                "type": "boolean"
                                            },
                                            "default_profile_image": {
                                                "type": "boolean"
                                            },
                                            "description": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "favourites_count": {
                                                "type": "long"
                                            },
                                            "followers_count": {
                                                "type": "long"
                                            },
                                            "friends_count": {
                                                "type": "long"
                                            },
                                            "geo_enabled": {
                                                "type": "boolean"
                                            },
                                            "id": {
                                                "type": "long"
                                            },
                                            "id_str": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "is_translator": {
                                                "type": "boolean"
                                            },
                                            "lang": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "listed_count": {
                                                "type": "long"
                                            },
                                            "location": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "name": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "profile_background_color": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "profile_background_image_url": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "profile_background_image_url_https": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "profile_background_tile": {
                                                "type": "boolean"
                                            },
                                            "profile_banner_url": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "profile_image_url": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "profile_image_url_https": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "profile_link_color": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "profile_sidebar_border_color": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "profile_sidebar_fill_color": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "profile_text_color": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "profile_use_background_image": {
                                                "type": "boolean"
                                            },
                                            "protected": {
                                                "type": "boolean"
                                            },
                                            "screen_name": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "statuses_count": {
                                                "type": "long"
                                            },
                                            "time_zone": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "url": {
                                                "fields": {
                                                    "keyword": {
                                                        "ignore_above": 256,
                                                        "type": "keyword"
                                                    }
                                                },
                                                "type": "text"
                                            },
                                            "utc_offset": {
                                                "type": "long"
                                            },
                                            "verified": {
                                                "type": "boolean"
                                            }
                                        }
                                    }
                                }
                            },
                            "quoted_status_id": {
                                "type": "long"
                            },
                            "quoted_status_id_str": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "retweet_count": {
                                "type": "long"
                            },
                            "retweeted": {
                                "type": "boolean"
                            },
                            "source": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "text": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "truncated": {
                                "type": "boolean"
                            },
                            "user": {
                                "properties": {
                                    "contributors_enabled": {
                                        "type": "boolean"
                                    },
                                    "created_at": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "default_profile": {
                                        "type": "boolean"
                                    },
                                    "default_profile_image": {
                                        "type": "boolean"
                                    },
                                    "description": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "favourites_count": {
                                        "type": "long"
                                    },
                                    "followers_count": {
                                        "type": "long"
                                    },
                                    "friends_count": {
                                        "type": "long"
                                    },
                                    "geo_enabled": {
                                        "type": "boolean"
                                    },
                                    "id": {
                                        "type": "long"
                                    },
                                    "id_str": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "is_translator": {
                                        "type": "boolean"
                                    },
                                    "lang": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "listed_count": {
                                        "type": "long"
                                    },
                                    "location": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "name": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "profile_background_color": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "profile_background_image_url": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "profile_background_image_url_https": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "profile_background_tile": {
                                        "type": "boolean"
                                    },
                                    "profile_banner_url": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "profile_image_url": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "profile_image_url_https": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "profile_link_color": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "profile_sidebar_border_color": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "profile_sidebar_fill_color": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "profile_text_color": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "profile_use_background_image": {
                                        "type": "boolean"
                                    },
                                    "protected": {
                                        "type": "boolean"
                                    },
                                    "screen_name": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "statuses_count": {
                                        "type": "long"
                                    },
                                    "time_zone": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "url": {
                                        "fields": {
                                            "keyword": {
                                                "ignore_above": 256,
                                                "type": "keyword"
                                            }
                                        },
                                        "type": "text"
                                    },
                                    "utc_offset": {
                                        "type": "long"
                                    },
                                    "verified": {
                                        "type": "boolean"
                                    }
                                }
                            }
                        }
                    },
                    "source": {
                        "fields": {
                            "keyword": {
                                "ignore_above": 256,
                                "type": "keyword"
                            }
                        },
                        "type": "text"
                    },
                    "text": {
                        "fields": {
                            "keyword": {
                                "ignore_above": 256,
                                "type": "keyword"
                            }
                        },
                        "type": "text"
                    },
                    "timestamp_ms": {
                        "fields": {
                            "keyword": {
                                "ignore_above": 256,
                                "type": "keyword"
                            }
                        },
                        "type": "text"
                    },
                    "truncated": {
                        "type": "boolean"
                    },
                    "user": {
                        "properties": {
                            "contributors_enabled": {
                                "type": "boolean"
                            },
                            "created_at": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "default_profile": {
                                "type": "boolean"
                            },
                            "default_profile_image": {
                                "type": "boolean"
                            },
                            "description": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "favourites_count": {
                                "type": "long"
                            },
                            "followers_count": {
                                "type": "long"
                            },
                            "friends_count": {
                                "type": "long"
                            },
                            "geo_enabled": {
                                "type": "boolean"
                            },
                            "id": {
                                "type": "long"
                            },
                            "id_str": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "is_translator": {
                                "type": "boolean"
                            },
                            "lang": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "listed_count": {
                                "type": "long"
                            },
                            "location": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "name": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "profile_background_color": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "profile_background_image_url": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "profile_background_image_url_https": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "profile_background_tile": {
                                "type": "boolean"
                            },
                            "profile_banner_url": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "profile_image_url": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "profile_image_url_https": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "profile_link_color": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "profile_sidebar_border_color": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "profile_sidebar_fill_color": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "profile_text_color": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "profile_use_background_image": {
                                "type": "boolean"
                            },
                            "protected": {
                                "type": "boolean"
                            },
                            "screen_name": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "statuses_count": {
                                "type": "long"
                            },
                            "time_zone": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "url": {
                                "fields": {
                                    "keyword": {
                                        "ignore_above": 256,
                                        "type": "keyword"
                                    }
                                },
                                "type": "text"
                            },
                            "utc_offset": {
                                "type": "long"
                            },
                            "verified": {
                                "type": "boolean"
                            }
                        }
                    }
                }
            }
    }
}

