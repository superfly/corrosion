FROM corrosion

RUN apt-get update && apt-get install -y curl socat python3

RUN apt install -y python3-pip python3-requests

RUN pip install antithesis cffi pg8000 --break-system-packages

COPY .antithesis/client/test-templates/parallel_driver* /opt/antithesis/test/v1/basic_test/

COPY .antithesis/client/test-templates/eventually_* /opt/antithesis/test/v1/basic_test/

COPY .antithesis/client/test-templates/anytime_* /opt/antithesis/test/v1/basic_test/

COPY .antithesis/client/config.toml /opt/antithesis/config.toml

COPY .antithesis/client/test-templates/py-resources/helper.py /opt/antithesis/py-resources/helper.py

COPY .antithesis/client/test-templates/check_bookkeeping.py /opt/antithesis/py-resources/check_bookkeeping.py

COPY .antithesis/client/entrypoint.sh /entrypoint.sh

RUN rm /opt/antithesis/test/v1/basic_test/parallel_driver_restore_corrosion_backup.sh

ENTRYPOINT ["/entrypoint.sh"]
