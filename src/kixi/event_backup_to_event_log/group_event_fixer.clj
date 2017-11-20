(ns kixi.event-backup-to-event-log.group-event-fixer)


(defn transform-old-group-event
  [old-payload]
  (-> old-payload
      (assoc :group-id (:id old-payload))
      (assoc :user-id (:created-by old-payload))
      (dissoc :id :created-by)))


(defn correct-event
  [event]
  (when-not (= "user" (get-in event [:kixi.comms.event/payload :group-type]))
    (if (= "group" (get-in event [:kixi.comms.event/payload :group-type]))
      (update event :kixi.comms.event/payload transform-old-group-event)
      (assoc-in event [:kixi.comms.event/payload :group-type] "group"))))

(defn group-created-event?
  [event]
  (and (not (:error event))
       (= :kixi.heimdall/group-created (:kixi.comms.event/key (:event event)))
       (= "2.0.0" (:kixi.comms.event/version (:event event)))))

(defn correct-group-created-events
  [event]
  (if (group-created-event? event)
    (when-let [new-event (correct-event (:event event))]
      (assoc event :event new-event))
    event))
