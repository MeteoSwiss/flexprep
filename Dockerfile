# Use a base image with the desired operating system and dependencies
ARG container_registry=dockerhub.apps.cp.meteoswiss.ch

FROM ${container_registry}/mch/python/builder:latest as mch-base

RUN apt-get -yqq update && apt-get install -yqq wget

# Install Miniconda
RUN wget -O ./miniconda.sh https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
RUN bash miniconda.sh -b -p /root/miniconda

ENV PATH=/root/miniconda/bin:$PATH

RUN conda config --set always_yes yes --set changeps1 no && \
    conda config --add channels conda-forge  && \
    conda config --set channel_priority strict 

# Install mamba package manager (faster than conda)
RUN conda install mamba -n base -c conda-forge

# Copy environment files
COPY environment.yml /scratch/environment.yml
# Create a mamba env based on the env.yml file
RUN mamba env create --prefix /opt/conda-env --file /scratch/environment.yml

# Public clone and install of meteodata-lab
ENV METEODATALAB_BRANCH=main
# Below ADD is needed to invalidate cache if meteodata-lab repo changes, and pip install latest version.
ADD https://api.github.com/repos/MeteoSwiss/meteodata-lab/git/refs/heads/${METEODATALAB_BRANCH} version_meteodatalab.json
RUN conda run -p /opt/conda-env /bin/bash -c "pip install https://github.com/MeteoSwiss/meteodata-lab/archive/refs/heads/${METEODATALAB_BRANCH}.zip"

# Set the working directory inside the container
WORKDIR /app

# Copy the necessary files from the host to the container
COPY app /app

# Activate the conda environment
ENV PATH /opt/conda-env/bin:$PATH

# Specify the command to run your data processing job
CMD ["conda", "run", "-p", "/opt/conda-env", "python", "app.py"]

