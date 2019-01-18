# Roof-top solar panel and cloud variation detection

Uses satellite data to identify roof-top solar panel objects and cloud coverage variation.

## Project Idea 

This project is especially concerned with identifying roof-top solar panels from satellite imagery and associating this with data of their surrounding sun light exposure intensity. 

## Tech Stack

Python/Dart

Kafka

gRPC/ProtoBuf

Flutter

S3

## Data Source

The data source for detecting roof-top solar panels will be Google earth engine while the data for cloud variaiton will be from Planet.com 

## Engineering Challenge

Efficient processing of video stream data to extract useful image features and piping this reliably to an object detcting ML model.


## Business Value

Energy harvest capacity predition.
Energy demand estimation.

## MVP

A system which detects the latest (upto 2 weeks late) distibution of roof-top solar panels and their corresponding sun light exposure for a sub-city section of San Fransico.

## Stretch Goals

General purpose realtime object detection and query system from a live view of satellite video stream.
