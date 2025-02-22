FROM confluentinc/cp-kafka@sha256:02170e46da5c36b1581cd42ebfbf2a55edae6348c62bc772bc2166841db745b2

HEALTHCHECK --interval=1s --timeout=30s --retries=30 CMD nc -z localhost 19092 || exit 1
