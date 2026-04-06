# Databricks notebook source
# MAGIC %sql
# MAGIC -- View data
# MAGIC SELECT * FROM my_data;
# MAGIC
# MAGIC -- Filter high salary
# MAGIC SELECT * 
# MAGIC FROM employee
# MAGIC WHERE salary > 500;
# MAGIC
# MAGIC -- Order by salary
# MAGIC SELECT id, name, salary
# MAGIC FROM employee
# MAGIC ORDER BY salary DESC;