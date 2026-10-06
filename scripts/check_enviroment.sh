#!/bin/bash

echo "===================================="
echo " BVL ANALYTICS - CHECK ENVIRONMENT"
echo "===================================="

echo
echo "===== JAVA ====="
java -version 2>&1

echo
echo "===== HADOOP ====="
hadoop version | head -n 2

echo
echo "===== SPARK ====="
spark-submit --version 2>&1 | head -n 8

echo
echo "===== PYTHON ====="
python3 --version

echo
echo "===== HADOOP_HOME ====="
echo "$HADOOP_HOME"

echo
echo "===== SPARK_HOME ====="
echo "$SPARK_HOME"

echo
echo "===== HADOOP / YARN PROCESSES ====="
jps

echo "===== HADOOP_CONF_DIR ====="
echo "$HADOOP_CONF_DIR"

echo "===== YARN_CONF_DIR ====="
echo "$YARN_CONF_DIR"

echo
echo "===================================="
echo " FIN DE LA VERIFICACION"
echo "===================================="
