#!/bin/bash

DOIT="spark-submit --class Impressions --master spark://$PUBLIC_DNS:7077 ./target/scala-2.10/impressions_2.10-1.0.jar"
echo $DOIT
`$DOTIT`
