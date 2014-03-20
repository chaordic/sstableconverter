#!/bin/bash

mvn clean compile assembly:single && echo "Success! Executable jar is on target directory."
