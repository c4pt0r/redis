#include "redis.h"
#include "cJSON.h"

// set or replace
int jsonTypeDocSet(robj *o, robj *doc) {
    cJSON *oldroot, *newroot;
    doc = getDecodedObject(doc);

    newroot = cJSON_Parse(doc->ptr);
    if (newroot == NULL) {
        decrRefCount(doc);
        return -1;
    }

    oldroot = o->ptr;
    o->ptr = newroot;
    if (oldroot) {
        cJSON_Delete(oldroot);
    }
    decrRefCount(doc);
    return 0;
}

int jsonWriteFieldVal(redisClient *c, robj *o, robj *param, robj *val) {
    int n, i;
    sds field;
    cJSON *node, *nextnode, *leaf, *oldleaf;
    sds* fields = sdssplitlen(param->ptr, strlen(param->ptr), ".", 1, &n);
    node = o->ptr;
    for (i = 0; i < n - 1; i++) {
        field = fields[i];
        nextnode = cJSON_GetObjectItem(node, field);
        if (nextnode == NULL) {
            // create it
            nextnode = cJSON_CreateObject();
            cJSON_AddItemToObject(node, field, nextnode);
        } else if (nextnode->type != cJSON_Object) {
            // replace it
            nextnode = cJSON_CreateObject();
            cJSON_ReplaceItemInObject(node, field, nextnode);
        }
        node = nextnode;
    }

    // check leaf node exists?
    oldleaf = cJSON_GetObjectItem(node, fields[n - 1]);
    // create new leaf
    val = getDecodedObject(val);
    leaf = cJSON_Parse(val->ptr);
    if (leaf == NULL) {
        leaf = cJSON_CreateString(val->ptr);
    }
    decrRefCount(val);

    if (leaf == NULL) {
        addReply(c, shared.czero);
        return -1;
    }

    // add leaf or replace origin leaf
    if (oldleaf == NULL) {
        cJSON_AddItemToObject(node, fields[n - 1], leaf);
    } else {
        cJSON_ReplaceItemInObject(node, fields[n - 1], leaf);
    }

    addReply(c, shared.cone);
    return 0;
}

void jsonReadFieldVal(redisClient *c, robj *o, robj* param) {
    int n, i;
    sds field;
    cJSON *root;
    char* buf;
    sds* fields = sdssplitlen(param->ptr, strlen(param->ptr), ".", 1, &n);
    root = o->ptr;
    for (i = 0; i < n; i++) {
        field = fields[i];
        root = cJSON_GetObjectItem(root, field);
        sdsfree(field);
        if (!root) {
            addReplyError(c, "no such field");
            return ;
        }
    }
    switch(root->type) {
        case cJSON_Number:
            addReplyLongLong(c, root->valueint);
            break;
        case cJSON_Object:
        case cJSON_String:
        case cJSON_True:
        case cJSON_False:
        case cJSON_Array:
        case cJSON_NULL:
            buf = cJSON_PrintUnformatted(root);
            addReplyBulkCString(c, buf);
            zfree(buf);
            break;
        default:
            addReplyError(c, "unknown");
            break;
    }
    return ;
}

robj *createJsonObject(void) {
    robj *o = createObject(REDIS_JSON, NULL);
    o->encoding = REDIS_ENCODING_JSON;
    return o;
}

robj *jsonTypeLookupOrCreate(redisClient *c, robj *key) {
    robj *o = lookupKeyWrite(c->db, key);
    if (o == NULL) {
        o = createJsonObject();
        dbAdd(c->db, key, o);
    } else {
        if (o->type != REDIS_JSON) {
            addReply(c, shared.wrongtypeerr);
            return NULL;
        }
    }
    return o;
}

/* commands */
void jdeldocCommand(redisClient *c) {
    robj *o;
    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_JSON)) return;
    if (dbDelete(c->db,c->argv[1])) addReply(c,shared.cone);
    addReply(c, shared.czero);
}

void jgetdocCommand(redisClient *c) {
    robj *o;
    cJSON *root;
    char *rendered;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) == NULL ||
        checkType(c,o,REDIS_JSON)) return;

    root = o->ptr;
    if (root) {
        rendered = cJSON_PrintUnformatted(root);
        addReplyBulkCBuffer(c, rendered, strlen(rendered));
        zfree(rendered);
    }
}

void jgetCommand(redisClient *c) {
    robj *o;

    o = jsonTypeLookupOrCreate(c, c->argv[1]);
    if (o == NULL) {
        return ;
    }

    jsonReadFieldVal(c, o, c->argv[2]);
}

void jsetCommand(redisClient *c) {
     robj *o;

    o = jsonTypeLookupOrCreate(c, c->argv[1]);
    if (o == NULL) {
        return ;
    }

    jsonWriteFieldVal(c, o, c->argv[2], c->argv[3]);
}

void jsetdocCommand(redisClient *c) {
    int ret;
    robj *o;

    o = jsonTypeLookupOrCreate(c, c->argv[1]);
    if (o == NULL) {
        return ;
    }

    ret = jsonTypeDocSet(o, c->argv[2]);
    if (ret == -1) {
        addReply(c, shared.czero);
        return ;
    }
    addReply(c, shared.cone);
}
