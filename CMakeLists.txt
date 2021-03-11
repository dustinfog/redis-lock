cmake_minimum_required(VERSION 3.17)
project(redis_lock C)

set(CMAKE_C_STANDARD 99)

add_library(redis_lock SHARED lock.c redismodule.h)