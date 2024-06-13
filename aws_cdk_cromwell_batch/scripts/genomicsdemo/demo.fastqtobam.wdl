version 1.0

workflow fastqtobam{
    
    input{
        File fastq1
        File fastq2
        String Ref
    }

    call bwa_mem { input: fastq1 = fastq1, fastq2 = fastq2, Ref = Ref }
    call samtobam { input: sam = bwa_mem.sam }

    output {
        File bam_file = samtobam.bam
    }
}

task bwa_mem{

    input{
        File fastq1
        File fastq2
        String Ref
    }

    command{
        bwa mem "/data/ref/${Ref}" -t 5  ${fastq1} ${fastq2} > sampledemo.sam
    }

    runtime {
        docker: "quay.io/refgenomics/docker-bwa"
    }

    output {
        File sam = "sampledemo.sam"
    }
}

task samtobam{

    input{
        File sam
    }

    command {
        samtools view -bS ${sam} -o sampledemo.bam
    }

    runtime {
        docker: "quay.io/refgenomics/docker-bwa"
    }

    output {
        File bam = "sampledemo.bam"
    }
}