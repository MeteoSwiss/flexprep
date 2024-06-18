FROM dockerhub.apps.cp.meteoswiss.ch/mch/python/builder:latest as builder

COPY poetry.lock pyproject.toml /src/app-root/

RUN cd /src/app-root \
    && poetry export -o requirements.txt --without-hashes \
    && poetry export --dev -o requirements_dev.txt --without-hashes


FROM dockerhub.apps.cp.meteoswiss.ch/mch/python-3.11:latest-slim AS base

COPY --from=builder /src/app-root/requirements.txt /src/app-root/requirements.txt

RUN apt-get -yqq update && apt-get install -yqq wget libeccodes-dev

RUN cd /src/app-root \
    && pip install -r requirements.txt

COPY pilotecmwf_pp_starter /src/app-root/pilotecmwf_pp_starter

WORKDIR /src/app-root

CMD ["python", "-m", "pilotecmwf_pp_starter"]

FROM base AS tester

COPY --from=builder /src/app-root/requirements_dev.txt /src/app-root/requirements_dev.txt
RUN pip install -r /src/app-root/requirements_dev.txt


COPY pyproject.toml test_ci.sh /src/app-root/
COPY test /src/app-root/test

CMD ["/bin/bash", "-c", "source /src/app-root/test_ci.sh && run_ci_tools"]

FROM tester AS documenter

COPY doc /src/app-root/doc
COPY HISTORY.rst README.rst /src/app-root/

CMD ["sphinx-build", "doc", "doc/_build"]

FROM base AS runner

ARG VERSION
ENV VERSION=$VERSION

# For running outside of OpenShift, we want to make sure that the container is run without root privileges
# uid 1001 is defined in the base-container-images for this purpose
USER 1001

CMD ["python", "-m", "pilotecmwf_pp_starter"]
