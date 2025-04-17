FROM corrosion

RUN apt-get update && apt-get install -y curl socat python3 vim

COPY .antithesis/client/test/ /opt/antithesis/test/

ENTRYPOINT ["sleep", "infinity"]
