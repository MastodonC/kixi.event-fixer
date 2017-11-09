(ns kixi.partition-keys)

(defn uuid
  [_]
  (str (java.util.UUID/randomUUID)))

(def event-type-version->partition-key-fn
  {[:kixi.datastore.file-metadata/updated "1.0.0"] #(or (get-in % [:kixi.comms.event/payload
                                                                   :kixi.datastore.metadatastore/file-metadata
                                                                   :kixi.datastore.metadatastore/id])
                                                        (get-in % [:kixi.comms.event/payload
                                                                   :kixi.datastore.metadatastore/id]))
   [:kixi.datastore.filestore/upload-link-created "1.0.0"] #(get-in % [:kixi.comms.event/payload :kixi.datastore.filestore/id])
   [:kixi.heimdall/user-logged-in "1.0.0"] uuid
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
   [:kixi.datastore.filestore/download-link-rejected "1.0.0"] #(get-in % [:kixi.comms.event/payload :kixi.datastore.metadatastore/id])


   [:kixi.heimdall/group-created "1.0.0"] #(get-in % [:kixi.comms.event/payload :user-id])
   [:kixi.heimdall/user-created "1.0.0"] uuid
   [:kixi.heimdall/invite-created "1.0.0"] uuid
   [:kixi.mailer/mail-accepted "1.0.0"] uuid
   [:kixi.mailer/mail-rejected "1.0.0"] uuid
   [:kixi.heimdall/member-added "1.0.0"] #(get-in % [:kixi.comms.event/payload :group-id])
   [:kixi.heimdall/member-added-failed "1.0.0"]  #(or (get-in % [:kixi.comms.event/payload :user-id])
                                                      (get-in % [:kixi.comms.event/payload :group-id]))
   [:kixi.heimdall/password-reset-request-created "1.0.0"] #(get-in % [:kixi.comms.event/payload :user :id])
   [:kixi.heimdall/invite-failed "1.0.0"] uuid
   [:kixi.heimdall/password-reset-completed "1.0.0"] uuid
   [:kixi.heimdall/password-reset-request-rejected "1.0.0"] uuid
   [:kixi.heimdall/user-created "2.0.0"]  #(get-in % [:kixi.comms.event/payload :id])
   [:kixi.heimdall/group-created "2.0.0"]  #(get-in % [:kixi.comms.event/payload :group-id])
   [:kixi.heimdall/create-group-failed "2.0.0"]  #(or (get-in % [:kixi.comms.event/payload :group-id])
                                                      (get-in % [:kixi.comms.event/payload :user-id]))

   })

(def event->event-type-version
  (juxt #(or (:kixi.event/type %) (:kixi.comms.event/key %))
        #(or (:kixi.event/version %) (:kixi.comms.event/version %))))

(defn new-format-datastore-event?
  [event]
  (some-> event
          :kixi.event/type
          namespace
          (= "kixi.datastore")))

(defn event->partition-key
  [{:keys [event] :as full}]
  (if-let [partition-key-fn (->> event
                                 event->event-type-version
                                 event-type-version->partition-key-fn)]
    (partition-key-fn event)
    (if (new-format-datastore-event? event)
      (if-let [partition-key (or (:kixi.datastore.metadatastore/id event)
                                 (:kixi.datastore.schemastore/id event))]
        partition-key
        (prn "Unknown event type: " (event->event-type-version event) "-" full))
      (prn "Unknown event type: " (event->event-type-version event) "-" full))))
