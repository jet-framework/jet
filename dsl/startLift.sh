#!/bin/sh
python lift_user_class.py impls/ structs/

cp impls/ApplicationOps.scala src/test/scala/

cd ..; sbt dsl/test:scalariform-format ; cd -
