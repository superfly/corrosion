FROM corrosion

RUN apt-get update && apt-get install -y curl socat python3 

RUN apt install -y python3-pip python3-requests

RUN pip install antithesis cffi --break-system-packages

COPY .antithesis/client/test/ /opt/antithesis/test/

COPY .antithesis/client/entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
