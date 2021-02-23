import sys

pipeline_in=sys.argv[1]
pipeline_out=sys.argv[2]

print(pipeline_in)
print(pipeline_out)

print("Starting the pipeline cleaning...")
preprocess(pipeline_in, pipeline_out)
print("Completed data cleaning!")