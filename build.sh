#!/bin/bash

mvn clean compile -U assembly:single && echo "Success! Executable jar is on target directory."
