FROM minio/minio:RELEASE.2023-06-29T05-12-28Z

RUN microdnf update \
 && microdnf install --nodocs wget unzip \
 && microdnf clean all \
 && rm -rf /var/cache/yum

RUN wget https://dl.min.io/client/mc/release/linux-amd64/mc  -P /usr/local/bin/
RUN chmod +x /usr/local/bin/mc

COPY entrypoint.sh .
RUN chmod +x ./entrypoint.sh

ENTRYPOINT [ "/entrypoint.sh" ]