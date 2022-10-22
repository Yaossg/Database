cmake_minimum_required(VERSION 3.21)
project(Database C)

set(CMAKE_C_STANDARD 11)

add_executable(Database main.c database.h database.c)
