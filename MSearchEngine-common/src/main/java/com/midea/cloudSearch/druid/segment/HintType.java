package com.midea.cloudSearch.druid.segment;



public enum HintType
{
    HASH_WITH_TERMS_FILTER,
    JOIN_LIMIT,
    USE_NESTED_LOOPS,
    NL_MULTISEARCH_SIZE,
    USE_SCROLL,
    IGNORE_UNAVAILABLE,
    DOCS_WITH_AGGREGATION;
}
