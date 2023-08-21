# dbproject2
Repo for CSCI 4370/6370 Project 2

CSCI X370: Database Management Summer 2023

Authors : Lokesh Adusumilli (811314401) ; Prudhvi Arun Chekka(811072949) ; Lalithya sajja(811167189) ; Dahun Im (811009835)

The 2 relational operations using an indexed algorithm are implemented in the "Table.java". They are

Select
Join (equi join)

We implemented the indexing using the linear hash map. We implemented 2 methods which are

set
split

The testing will start in MovieDB.java, the entry point for the project used for testing.
We have performed unit testing using J-unit to test the individual relational operators.
The test cases are written in "JunitTest_project2.java" for all the relational operations.
All the test cases are validated with the expected output tables.
To start the unit testing test cases, we must run the "JunitTest_project2.java". (before running the file, please make sure that all the necessary packages are available) Note: make sure that "JunitTest_project2.java" exists in the source folder.

We have also implemented the methods in FileList.java which are

add
get
