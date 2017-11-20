(ns kixi.event-backup-to-event-log.event-filter)



(comment "Officially our history began on the 18th of April 2017, however there was some testing from before that period. Those tests have left some rough patches of data in the event history. Here we'll ditch anything to do with those tests and it'll be like they never happened.")




(def unwanted-ids #{"bdd8b92e-c6df-4498-9ca4-a5778693f972"
                    "aa277eff-b130-4993-8ec9-d43a9fdb25c6"
                    "267b9ce0-0983-4675-bf0f-a8f71603f853"
                    "34fd5809-7444-4966-a18a-47a272e6407f"
                    "e1118499-d0f7-4eb4-a837-dc84e86fcc6b"
                    "62a4e3ca-726d-4e3a-919d-dbf69fd1d8a4"

                    "16f6efeb-58db-4ed5-888f-75df5ff1e1a6"})

(def extractors
  {[:kixi.datastore.file-metadata/updated "1.0.0"] #(or (get-in % [:kixi.comms.event/payload
                                                                   :kixi.datastore.metadatastore/file-metadata
                                                                   :kixi.datastore.metadatastore/id])
                                                        (get-in % [:kixi.comms.event/payload
                                                                   :kixi.datastore.metadatastore/id]))
   [:kixi.datastore.filestore/upload-link-created "1.0.0"] #(get-in % [:kixi.comms.event/payload :kixi.datastore.filestore/id])
   [:kixi.datastore.file/created "1.0.0"] #(get-in % [:kixi.comms.event/payload :kixi.datastore.metadatastore/id])
   [:kixi.datastore.metadatastore/update-rejected "1.0.0"] #(or (get-in % [:kixi.comms.event/payload :kixi.datastore.metadatastore/id])
                                                                (get-in % [:kixi.comms.event/payload
                                                                           :original
                                                                           :kixi.datastore.metadatastore/payload
                                                                           :kixi.comms.command/payload
                                                                           :kixi.datastore.metadatastore/id]))
   [:kixi.datastore.filestore/download-link-created "1.0.0"] #(get-in % [:kixi.comms.event/payload :kixi.datastore.metadatastore/id])
   [:kixi.datastore.metadatastore/sharing-change-rejected "1.0.0"] #(get-in % [:kixi.comms.event/payload :kixi.datastore.metadatastore/id])
   [:kixi.datastore.file-metadata/rejected "1.0.0"] #(get-in % [:kixi.comms.event/payload
                                                                :kixi.datastore.metadatastore/file-metadata
                                                                :kixi.datastore.metadatastore/id])
   [:kixi.datastore.filestore/download-link-rejected "1.0.0"] #(get-in % [:kixi.comms.event/payload :kixi.datastore.metadatastore/id])})

(def event->event-type-version
  (juxt #(or (:kixi.event/type %) (:kixi.comms.event/key %))
        #(or (:kixi.event/version %) (:kixi.comms.event/version %))))

(defn questionable-event-type?
  [event]
  (get extractors
       (event->event-type-version event)))

(defn extract-metadata-id
  [event]
  ((get extractors
        (event->event-type-version event))
   event))

(defn unwanted-event?
  [{:keys [event]}]
  (when (questionable-event-type? event)
    (unwanted-ids (extract-metadata-id event))))
