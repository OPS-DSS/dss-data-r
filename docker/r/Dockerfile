FROM rocker/geospatial:4.5

LABEL maintainer="Diego Vargas <vargasdie@paho.org>"
LABEL description="R environment for DSS indicators processing"

# Set working directory
WORKDIR /workspace

# Install every dependency declared in DESCRIPTION (Imports + Suggests) plus
# the few packages that only the scripts/setup use, so a fresh image can run
# any npm r:* command without r:setup. Packages already in rocker/geospatial
# (tidyverse, sf, terra, devtools...) are kept as they are.
COPY DESCRIPTION /tmp/dss.indicators/DESCRIPTION
RUN Rscript -e "remotes::install_deps('/tmp/dss.indicators', dependencies = TRUE, upgrade = 'never')" \
 && Rscript -e "install.packages(setdiff(c('jsonlite', 'renv', 'whisker'), rownames(installed.packages())))" \
 && rm -rf /tmp/dss.indicators

# Default command
CMD ["R"]
