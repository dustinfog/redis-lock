#include "redismodule.h"
#include <stdio.h>
#include <ctype.h>
#include <string.h>

int reply_friendly(RedisModuleCtx *ctx, int successful, const char *msg, size_t len) {
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithLongLong(ctx,successful);
    return RedisModule_ReplyWithString(ctx, RedisModule_CreateString(ctx, msg, len));
}

int same_lock(const char *info, const char *token, size_t token_len) {
    char *c = strchr(info, '|');
    if (c == NULL) {
        return 0;
    }
    if (c - info != token_len) {
        return 0;
    }

    return memcmp(info, token, token_len) == 0;
}

int lock_acquire(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    // 参数处理
    if (argc != 5) return RedisModule_WrongArity(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
                                              REDISMODULE_READ|REDISMODULE_WRITE);
    size_t token_len;
    const char *token = RedisModule_StringPtrLen(argv[2], &token_len);
    size_t reason_len;
    const char *reason = RedisModule_StringPtrLen(argv[3], &reason_len);
    mstime_t ttl;
    if ((RedisModule_StringToLongLong(argv[4],&ttl) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
    }

    // 返回值声明
    int ret;
    RedisModuleString *info;

    // 分类型处理
    int keytype = RedisModule_KeyType(key);
    if (keytype == REDISMODULE_KEYTYPE_EMPTY) {
        goto lock;
    }

    if (keytype == REDISMODULE_KEYTYPE_STRING) {
        size_t len;
        const char *raw_info = RedisModule_StringDMA(key, &len, REDISMODULE_READ);
        if (same_lock(raw_info, token, token_len)) {
            return reply_friendly(ctx, 1, "", 0);
        }

        return reply_friendly(ctx, 0, raw_info, len);
    }

    if (keytype == REDISMODULE_KEYTYPE_HASH) {
        RedisModuleCallReply *reply = RedisModule_Call(ctx, "HGETALL", "s", argv[1]);
        int reply_type = RedisModule_CallReplyType(reply);
        if(reply_type != REDISMODULE_REPLY_ARRAY) {
            char buf[50];
            sprintf(buf, "invalid lock info: %d", reply_type);
            return RedisModule_ReplyWithError(ctx, buf);
        }

        size_t reply_len = RedisModule_CallReplyLength(reply);
        for (size_t i = 1; i < reply_len; i += 2) {
            RedisModuleCallReply *sub_reply = RedisModule_CallReplyArrayElement(reply, i);
            size_t len;
            const char *raw_info = RedisModule_CallReplyStringPtr(sub_reply, &len);

            if (!same_lock(raw_info, token, token_len)) {
                return reply_friendly(ctx, 0, raw_info, len);
            }
        }

        RedisModule_DeleteKey(key);
        goto lock;
    }

    return RedisModule_ReplyWithError(ctx, "wrong key type");

lock:
    info = RedisModule_CreateStringPrintf(ctx, "%s|%s", token, reason);
    ret = RedisModule_StringSet(key, info);
    if (ret != REDISMODULE_OK) {
        return RedisModule_ReplyWithError(ctx, "lock fail");
    }

    ret = RedisModule_SetExpire(key, ttl);

    if (ret != REDISMODULE_OK) {
        return RedisModule_ReplyWithError(ctx, "set ttl fail");
    }

    return reply_friendly(ctx, 1, "", 0);
}

int lock_release(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    // 参数处理
    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
                                              REDISMODULE_READ|REDISMODULE_WRITE);
    size_t token_len;
    const char *token = RedisModule_StringPtrLen(argv[2], &token_len);

    // 分类型处理
    int keytype = RedisModule_KeyType(key);

    if (keytype == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithLongLong(ctx, 1);
    }

    if (keytype == REDISMODULE_KEYTYPE_STRING) {
        size_t len;
        const char *raw_info = RedisModule_StringDMA(key, &len, REDISMODULE_READ);
        if (same_lock(raw_info, token, token_len)) {
            RedisModule_DeleteKey(key);
            return RedisModule_ReplyWithLongLong(ctx, 1);
        }
    }

    return RedisModule_ReplyWithLongLong(ctx, 0);
}

int lock_sub_acquire(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    // 参数处理
    if (argc != 6) return RedisModule_WrongArity(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
                                              REDISMODULE_READ|REDISMODULE_WRITE);

    RedisModuleString *sub_key = argv[2];
    size_t token_len;
    const char *token = RedisModule_StringPtrLen(argv[3], &token_len);
    size_t reason_len;
    const char *reason = RedisModule_StringPtrLen(argv[4], &reason_len);
    mstime_t ttl;
    if ((RedisModule_StringToLongLong(argv[5],&ttl) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
    }

    // 返回值声明
    int ret;
    RedisModuleString *info;

    // 分类型处理
    int keytype = RedisModule_KeyType(key);
    if (keytype == REDISMODULE_KEYTYPE_EMPTY) {
        goto lock;
    }

    if (keytype == REDISMODULE_KEYTYPE_STRING) {
        size_t len;
        const char *raw_info = RedisModule_StringDMA(key, &len, REDISMODULE_READ);
        if (same_lock(raw_info, token, token_len)) {
            return reply_friendly(ctx, 1, "", 0);
        }

        return reply_friendly(ctx, 0, raw_info, len);
    }

    if (keytype == REDISMODULE_KEYTYPE_HASH) {
        RedisModuleString *origin_info;
        RedisModule_HashGet(key, REDISMODULE_HASH_NONE, sub_key, &origin_info, NULL);

        if (origin_info == NULL) {
            goto lock;
        }

        size_t origin_info_len;
        const char *raw_origin_info = RedisModule_StringPtrLen(origin_info, &origin_info_len);
        if (same_lock(raw_origin_info, token, token_len)) {
            return reply_friendly(ctx, 1, "", 0);
        }

        return reply_friendly(ctx, 0, raw_origin_info, origin_info_len);
    }

    return RedisModule_ReplyWithError(ctx, "wrong key type");
lock:
    info = RedisModule_CreateStringPrintf(ctx, "%s|%s", token, reason);
    ret = RedisModule_HashSet(key, REDISMODULE_HASH_NONE, sub_key,info,NULL);
    if (ret != REDISMODULE_OK) {
        return RedisModule_ReplyWithError(ctx, "lock fail");
    }

    ret = RedisModule_SetExpire(key, ttl);

    if (ret != REDISMODULE_OK) {
        return RedisModule_ReplyWithError(ctx, "set ttl fail");
    }

    return reply_friendly(ctx, 1, "", 0);
}

int lock_sub_release(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    // 参数处理
    if (argc != 4) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
                                              REDISMODULE_READ|REDISMODULE_WRITE);

    RedisModuleString *sub_key = argv[2];
    size_t token_len;
    const char *token = RedisModule_StringPtrLen(argv[3], &token_len);

    // 分类型处理
    int keytype = RedisModule_KeyType(key);

    if (keytype == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithLongLong(ctx, 1);
    }

    if (keytype == REDISMODULE_KEYTYPE_HASH) {
        RedisModuleString *origin_info;
        RedisModule_HashGet(key, REDISMODULE_HASH_NONE, sub_key, &origin_info, NULL);

        if (origin_info != NULL) {
            size_t origin_info_len;
            const char *raw_origin_info = RedisModule_StringPtrLen(origin_info, &origin_info_len);
            if (same_lock(raw_origin_info, token, token_len)) {
                RedisModule_HashSet(key, REDISMODULE_HASH_NONE, sub_key, REDISMODULE_HASH_DELETE, NULL);
                return RedisModule_ReplyWithLongLong(ctx, 1);
            }
        }
    }

    return RedisModule_ReplyWithLongLong(ctx, 0);
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx,"lock",1,REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"lock.acquire",
                                  lock_acquire,"write deny-oom",1,1,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"lock.release",
                                  lock_release,"write deny-oom",1,1,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"lock.sacquire",
                                  lock_sub_acquire,"write deny-oom",1,1,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx,"lock.srelease",
                                  lock_sub_release,"write deny-oom",1,1,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    /* Create our global dictionary. Here we'll set our keys and values. */

    return REDISMODULE_OK;
}
