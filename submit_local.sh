#!/bin/bash
spark-submit --master local[2] --class Impressions   target/scala-2.10/impressions_2.10-1.0.jar
