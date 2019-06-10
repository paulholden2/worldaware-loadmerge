# WorldAware load file merger

This tool scans a given PSIcapture output directory for load files (by a
specific name: Ingestion Load File.csv) and processes them, combining
multi-record rows (product names) into a pipe-delimited list.

Since only Client and Partner format files have a products field,
this tool will only process load files in those corresponding subfolders.
