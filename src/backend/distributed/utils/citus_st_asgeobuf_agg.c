/*-------------------------------------------------------------------------
 *
 * citus_st_asgeobuf_agg.c
 *
 * This file contains functions to implement geobuf data merge, so that the ST_AsGeobuf funcion
 * can run parallel in worker node.
 *
 * Added by Ecopia.
 *
 *-------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <math.h>

#include "distributed/citus_st_asgeobuf_agg.h"

PG_FUNCTION_INFO_V1(citus_st_asgeobuf_agg_transfn);
PG_FUNCTION_INFO_V1(citus_st_asgeobuf_agg_finalfn);

static void * geobuf_allocator(__attribute__((__unused__)) void *data, size_t size) {
	return palloc(size);
}

static void geobuf_deallocator(__attribute__((__unused__)) void *data, void *ptr) {
	return pfree(ptr);
}

size_t geobuf__data__feature__get_packed_size (const Geobuf__Data__Feature *message) {
    assert(message->base.descriptor == &geobuf__data__feature__descriptor);
    return protobuf_c_message_get_packed_size ((const ProtobufCMessage*)(message));
}

size_t geobuf__data__feature__pack(const Geobuf__Data__Feature *message,uint8_t *out) {
    assert(message->base.descriptor == &geobuf__data__feature__descriptor);
    return protobuf_c_message_pack ((const ProtobufCMessage*)message, out);
}

Geobuf__Data__Feature *geobuf__data__feature__unpack(ProtobufCAllocator *allocator,size_t len,const uint8_t *data) {
    return (Geobuf__Data__Feature *)protobuf_c_message_unpack (&geobuf__data__feature__descriptor, allocator, len, data);
}

Geobuf__Data__Feature * geobuf__data__feature__copy(Geobuf__Data__Feature *src, ProtobufCAllocator *allocator) {
    size_t buffer_len = geobuf__data__feature__get_packed_size(src);
    uint8_t *buffer = (uint8_t *)allocator->alloc(NULL, buffer_len);
    geobuf__data__feature__pack(src, buffer);
    Geobuf__Data__Feature *new_feature = geobuf__data__feature__unpack(allocator, buffer_len, buffer);
    allocator->free(NULL, buffer);
    return new_feature;
}

void geobuf__data__geometry__change_precision(Geobuf__Data__Geometry *geometry, int factor) {
    for(int i = 0; i < geometry->n_coords; ++i) {
        geometry->coords[i] = geometry->coords[i] * factor;
    }
}


void citus_geobuf_agg_context_add_data(struct citus_geobuf_agg_context *ctx, uint8_t *data_ptr, size_t data_len) {
    ctx->agg_array_size++;
    
    // if this is the first geobuf data, we just copy the bytes data to buf, 
    // skip unpack data, when receive the next geobuf data, then unpack them. 
    // In most case, our data case is only 1 geobuf data, so the just return
    // the bytes of the first data, skip unpack and pack.
    if(ctx->agg_array_size == 1) {
        ctx->buf = (uint8_t*) palloc(sizeof(uint8_t) * data_len);
        memcpy(ctx->buf, data_ptr, data_len);
        ctx->buf_len = data_len;
        return;    
    }

    // there is >= 2 geobuf data, so we need to unpack the data, and merge each of then.
    ProtobufCAllocator allocator = {
		geobuf_allocator,
		geobuf_deallocator,
		NULL
	};
    // if this is the second one, we add the first geobuf data which stored in the buf.
    if (ctx->agg_array_size == 2) {
        Geobuf__Data *pre_data = geobuf__data__unpack(&allocator, ctx->buf_len, ctx->buf);
        citus_geobuf_agg_context_merge_geobuf(ctx, pre_data);
        geobuf__data__free_unpacked(pre_data, &allocator);
        pre_data = NULL;
    }

    Geobuf__Data *cur_data = geobuf__data__unpack(&allocator, data_len, data_ptr);
    citus_geobuf_agg_context_merge_geobuf(ctx, cur_data);
    geobuf__data__free_unpacked(cur_data, &allocator);
    cur_data = NULL;
}

void citus_geobuf_agg_context_merge_geobuf(struct citus_geobuf_agg_context *ctx, Geobuf__Data *data) {
    ProtobufCAllocator allocator = {
		geobuf_allocator,
		geobuf_deallocator,
		NULL
	};

    if(ctx->data == NULL) {
        ctx->data = (Geobuf__Data *) palloc(sizeof(Geobuf__Data));
        geobuf__data__init(ctx->data);
        ctx->data->data_type_case = GEOBUF__DATA__DATA_TYPE_FEATURE_COLLECTION;

        Geobuf__Data__FeatureCollection *feature_collection = (Geobuf__Data__FeatureCollection*) palloc(sizeof(Geobuf__Data__FeatureCollection));
        geobuf__data__feature_collection__init(feature_collection);
        ctx->data->feature_collection = feature_collection;

        ctx->data->dimensions = 2;
        ctx->data->precision = 6;
    }

    // merge keys
    size_t pre_key_len = ctx->data->n_keys;
    size_t cur_key_len = data->n_keys;
    if (ctx->keys_real_size < pre_key_len + cur_key_len) {
        char **keys;
        if (pre_key_len == 0) {
            keys = (char**) palloc(sizeof(char*) * (pre_key_len + cur_key_len));
        }else {
            keys = (char**) repalloc(ctx->data->keys, sizeof(char*) * (pre_key_len + cur_key_len));
        }

        ctx->data->keys = keys;
        ctx->keys_real_size = pre_key_len + cur_key_len;
    }

    int *cur_key_index_map = (int *)palloc(cur_key_len * sizeof(int));
    for(int i = 0; i < cur_key_len; ++i) {
        int index = -1;
        for(int j = 0; j < pre_key_len; ++j) {
            if(strcmp(data->keys[i], ctx->data->keys[j])) {
                index = j;
                break;
            }
        }
        if(index == -1) {
            index = pre_key_len;
            char *key = pstrdup(data->keys[i]);
            ctx->data->keys[pre_key_len++] = key;
            ctx->data->n_keys++;
        }
        cur_key_index_map[i] = index;
    }

    //realloc feature in feature_collection
    Geobuf__Data__Feature **features_list;
    if (ctx->data->feature_collection->n_features > 0) {
        features_list = (Geobuf__Data__Feature **)repalloc( ctx->data->feature_collection->features, 
            sizeof(Geobuf__Data__Feature *) * (ctx->data->feature_collection->n_features + data->feature_collection->n_features));
    }else {
        features_list = (Geobuf__Data__Feature **)palloc( 
            sizeof(Geobuf__Data__Feature *) * (ctx->data->feature_collection->n_features + data->feature_collection->n_features));
    }
    ctx->data->feature_collection->features = features_list;

    // merge feature collection
    int precision_diff = ctx->data->precision - data->precision;
    int factor = (int) (pow(10, precision_diff) + 0.5);
    for(int i = 0; i < data->feature_collection->n_features; ++i) {
        Geobuf__Data__Feature *feature = data->feature_collection->features[i];
        Geobuf__Data__Feature *new_feature = geobuf__data__feature__copy(feature, &allocator);

        //remap key index to new index value
        for(int j = 0; j < new_feature->n_properties / 2; ++j) {
            new_feature->properties[j * 2] = cur_key_index_map[new_feature->properties[j * 2]];
        }

        // if the precision in new data is not 6, align the data precision to 6
        if(precision_diff != 0) {        
            geobuf__data__geometry__change_precision(new_feature->geometry, factor);
        }

        // add the new_feature
        ctx->data->feature_collection->features[ctx->data->feature_collection->n_features++] = new_feature;
    }
}


void citus_geobuf_agg_context_init(citus_geobuf_agg_context *ctx) {
    ctx->agg_array_size = 0;
    ctx->buf = NULL;
    ctx->precision = 6;
    ctx->dimensions = 2;
    ctx->keys_real_size = 0;
    ctx->data = NULL;
}

void citus_geobuf_agg_context_clean(citus_geobuf_agg_context *ctx) {
    if(ctx->buf != NULL) {
        pfree(ctx->buf);
        ctx->buf = NULL;
        ctx->buf_len = 0;
    }
    if(ctx->data != NULL) {
        if(ctx->data->feature_collection != NULL) {
            pfree(ctx->data->feature_collection);
            ctx->data->feature_collection = NULL;
        }
        pfree(ctx->data);
        ctx->data = NULL;
    }
}

Datum
citus_st_asgeobuf_agg_transfn(PG_FUNCTION_ARGS) {
    citus_geobuf_agg_context *ctx;

    if (PG_ARGISNULL(0)) {
        ctx = (citus_geobuf_agg_context *) palloc(sizeof(citus_geobuf_agg_context));
        citus_geobuf_agg_context_init(ctx);
    }else {
        ctx = (citus_geobuf_agg_context *) PG_GETARG_POINTER(0);
    }

    if (!PG_ARGISNULL(1)) {
        bytea *bytea_geobuf = PG_GETARG_BYTEA_P(1);
        uint8_t *geobuf = (uint8_t*)VARDATA(bytea_geobuf);
        size_t geobuf_len = VARSIZE_ANY_EXHDR(bytea_geobuf);
        citus_geobuf_agg_context_add_data(ctx, geobuf, geobuf_len);
    }

    PG_RETURN_POINTER(ctx);
}

Datum
citus_st_asgeobuf_agg_finalfn(PG_FUNCTION_ARGS) {
    if (PG_ARGISNULL(0)) {
        PG_RETURN_NULL();
    }

    uint8_t *buf;
    citus_geobuf_agg_context *ctx;

    ctx = (citus_geobuf_agg_context *) PG_GETARG_POINTER(0);
    if(ctx->agg_array_size == 0) {
        PG_RETURN_NULL();
    }else if(ctx->agg_array_size == 1) { 
        if(ctx->buf == NULL || ctx->buf_len == 0) {
            PG_RETURN_NULL();
        }

        buf = (uint8_t *)palloc(sizeof(uint8_t) * (ctx->buf_len + VARHDRSZ));
        memcpy(buf + VARHDRSZ, ctx->buf, sizeof(uint8_t) * ctx->buf_len);
        SET_VARSIZE(buf, ctx->buf_len + VARHDRSZ);

        citus_geobuf_agg_context_clean(ctx);

        PG_RETURN_BYTEA_P(buf);
    }

    // generate result data
    int packed_size = geobuf__data__get_packed_size(ctx->data);
    buf = (uint8_t *)palloc(sizeof(uint8_t) * (packed_size + VARHDRSZ));
    geobuf__data__pack(ctx->data, buf + VARHDRSZ);
    SET_VARSIZE(buf, packed_size + VARHDRSZ);

    citus_geobuf_agg_context_clean(ctx);

    PG_RETURN_BYTEA_P(buf);
}

