import quilt3 as q3
b = q3.Bucket("s3://janelia-cosem-datasets")
# List files[?]
b.ls("jrc_hela-1/")
# Download[?]
b.fetch("jrc_hela-1/", "./jrc_hela-1")
