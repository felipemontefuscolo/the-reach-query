#!/bin/bash
spark-submit --master spark://$(hostname):7077 --class impressions   target/scala-2.10/impressions_2.10-1.0.jar
