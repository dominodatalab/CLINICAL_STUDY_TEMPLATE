touch /mnt/data/$DOMINO_PROJECT_NAME/snakemake.rst
snakemake --cores 1 --directory "/mnt/data/$DOMINO_PROJECT_NAME/" && snakemake --report "/mnt/data/$DOMINO_PROJECT_NAME/report.html" --directory "/mnt/data/$DOMINO_PROJECT_NAME/"
