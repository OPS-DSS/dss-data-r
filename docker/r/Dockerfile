FROM rocker/geospatial:4.5

LABEL maintainer="Diego Vargas <vargasdie@paho.org>"
LABEL description="R environment for DSS indicators processing"

# Set working directory
WORKDIR /workspace

# geodata and RColorBrewer are not included in rocker/geospatial
# (terra, sf, tidyverse are already pre-installed in the base image)
RUN Rscript -e "install.packages(c('here', 'geodata', 'RColorBrewer'), repos='https://cran.rstudio.com/')"

# Default command
CMD ["R"]
