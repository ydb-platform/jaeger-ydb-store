# Examples


1. Select which Yandex Database flavour you will be using (dedicated or serverless).
2. Edit proper docker-compose configuration, specifying full database path and endpoint to connect to.
3. Create service account with either `editor` or `ydb.admin` role assigned.
4. Deploy VM with the SA previously created attached to this VM.
   Also, you will need to verify proper network conditions:
   * Dedicated: either deploy in the same network
   (if Yandex Database isn't configured to provide external IP address)
   or with Internet access.
   * Serverless: deploy onto VM with Internet access.