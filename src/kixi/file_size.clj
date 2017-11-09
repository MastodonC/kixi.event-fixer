(ns kixi.file-size
  (:require [amazonica.aws.s3 :as s3]
            [kixi.maws :refer [witan-prod-creds]]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [cheshire.core :as json])
  (:import [java.io ByteArrayInputStream File InputStream]))

(def dynamo-backup-bucket "prod.witan-dynamo-backup")

(def dynamo-backup-prefix "kixi.datastore/2017-10-18-04-26-10/prod-kixi.datastore-metadatastore-")

(def backup-local-dir "metadatastore-backup")

(def credentials (assoc (witan-prod-creds) :client-config {:max-connections 50
                                                           :connection-timeout 5000
                                                           :socket-timeout 5000}))

(defn download-backup
  [s3-object-summary]
  (let [^File local-file (io/file backup-local-dir (last (string/split (:key s3-object-summary) #"/")))]
    (when-not (.exists local-file)
      (do (.createNewFile local-file)
          (let [s3-object (s3/get-object credentials
                                         :bucket-name dynamo-backup-bucket
                                         :key (:key s3-object-summary))]
            (with-open [^InputStream in (:input-stream s3-object)]
              (io/copy in local-file)))))
    local-file))

(defn decode-file
  [file-name]
  (with-open [in (io/input-stream file-name)]
    (json/parse-stream (io/reader in) true)))

(defn parse-int
  [item]
  (if-let [s ((comp :N :kixi.datastore.metadatastore_size-bytes) item)]
    (Integer/parseInt s)
    (do
      (prn item)
      0)))

(defn extract-items
  [file-contents]
  (for [put-req (:prod-kixi.datastore-metadatastore file-contents)]
    (->> put-req
         :PutRequest
         :Item)))

(defn bundle?
  [item]
  (= "bundle"
     (:S (:kixi.datastore.metadatastore_type item))))

(comment
  (into {}
        (comp
         (map download-backup)
         (map decode-file)
         (mapcat extract-items)
         (remove bundle?)
         (map (juxt (comp :S :kixi.datastore.metadatastore_id)
                    parse-int)))
        (:object-summaries (s3/list-objects credentials (merge {:bucket-name dynamo-backup-bucket
                                                                :prefix dynamo-backup-prefix
                                                                :max-keys 20})))))

(def file-id->size
  {"6f47f029-b6e1-4645-aea1-aed4fc76ee7f" 42954752,
   "f040f3e6-fe7f-4233-91c6-a9dac40c2755" 1136076, "8d423212-03e0-460c-af41-d76b848965c0" 3676555, "428af18c-80ee-4579-9ff9-650ae6ccd31b" 1692973, "903d35ff-dfc0-41b3-8013-014c15342814" 27832, "0bb1e76a-8806-4263-9486-919b4c064bcf" 32294986, "bdd8b92e-c6df-4498-9ca4-a5778693f972" 2690628, "9ff2287b-a14e-4cb3-987f-6ff775f52068" 14, "759de9b5-7efb-4d11-a762-97b657841015" 209026, "603620d1-1351-4c94-b0a0-587d8e19d7c1" 348160, "4fcfd57c-3d4d-40cc-baba-7b3c8d2312e6" 41222, "60c17ea5-add1-4b36-b4de-6c73cc1f3704" 23373770, "feaf0740-295f-43d0-9fda-209836c7f103" 14, "c7f79198-a2c2-4021-a435-8b9b47e6f594" 391628, "6b251f93-60a9-40eb-8ce2-9ba1c05fed8c" 14, "43a40b91-fb14-4bb4-9a99-70cc419ab10a" 55089, "29549c95-eaab-417d-bf11-2ea5fd1f93b6" 7693006, "c2059566-c573-44ae-9af3-e3c60782308e" 36045369, "aa277eff-b130-4993-8ec9-d43a9fdb25c6" 5903393, "aec08499-00ac-4413-a92e-61fd05c05f23" 388600, "4d8ae1cc-65e1-48ba-abc9-f1cc0665c67d" 49, "81f1e77e-3c08-4cde-8aad-8aa6f1907bba" 47778, "23976537-7926-42f9-8ce1-1fd1cc032b91" 25671994, "3200920d-fcb5-44a2-af69-01403d02e5dd" 149, "2b134e07-7ba0-44b8-93aa-ac2907607da8" 10135, "c005744f-6b21-46d6-aa73-4b5114a78a35" 24437, "fb677d6f-93b4-4c4c-86b7-bb11445fad69" 26319, "2f6d3909-3daa-44b4-a3ad-d32ae9642a18" 5473968, "267b9ce0-0983-4675-bf0f-a8f71603f853" 14, "02a40939-8bc2-4aed-b3d3-ae459223ce7c" 23112, "fee3dc44-6243-43d7-b7c7-c12d2f3ab6df" 464660, "aed62a72-fc6e-4630-aceb-b63a7468b4aa" 48542, "65adf4ff-93d6-4524-b1b0-f6b25f093651" 59733, "375f0e75-1368-4e60-8024-c79131052204" 381175, "735f287c-c9fa-4cbe-a556-58eb4d425ecb" 20408, "3da5786f-9f8d-4c81-bddc-feae3b55cfe2" 27732, "6904c667-a2f2-4903-8ecc-3005fd7898ef" 226452, "34fd5809-7444-4966-a18a-47a272e6407f" 2690628, "b8b374bd-cb41-4b90-9fd5-a7f8b3939747" 27730, "3495a1ab-64cc-45e7-8363-2f516fc6315b" 48986, "3a57d2ed-96b9-4f4b-aac5-45048f473dca" 2726530, "f73f9532-bd58-4ca3-8f60-8c102978d7ae" 148109551, "2722c810-1fab-474d-a2c5-dc66052ae648" 80946, "f73e8fe0-079d-4d3f-8670-779cf53d4c00" 6371, "12b6e077-8945-43e8-8db4-fc09210ed16f" 72305, "dbf9f1ef-6d66-445a-8c68-9d3f4516006d" 24395, "001428ed-134f-4060-ad60-45cfed67d553" 582373, "85aab097-4a99-4705-8338-9223c8121656" 244177, "f4e066ba-4e71-4674-a9ca-b9ce1c8af57c" 31753, "eb529144-ccd2-4ff2-b72b-b594090bacc8" 29594198, "dd8fc8fa-5ee1-429c-ba0a-e98b7b3ef2b7" 97989, "9ff72b65-131d-497b-8b31-8fa687232690" 14, "825ad87b-80c8-45ce-9c3c-a3dffee2ffb3" 7840, "e14b9f05-5bbc-4acf-8fcb-d8f815debc70" 490517, "791932d7-89e5-4749-835c-d0c049b120ee" 80946, "16f6efeb-58db-4ed5-888f-75df5ff1e1a6" 248639414, "9d578625-b326-434e-850e-bd496879c851" 34394, "141d16fd-0fa1-47b7-94c3-ac7150a349f2" 154563, "d7eb9530-a9fb-4b7f-bf9c-98b7f6a0bf3e" 81996, "ab136da1-2dbb-4bd2-9430-53e7fd9b1707" 35114370, "6d3e022d-ce56-40a4-b284-155cf4322c60" 71044235, "fce2a32e-afaa-4931-8201-82d7b5797f9b" 41222, "29296e72-b093-4e6c-bd84-e2c79e578aa6" 42860544, "9f003a7e-9117-4ffd-aabc-c9529e4a57b6" 1082115, "4a3575c5-293f-4090-80a5-5c4ea807c164" 39495, "471f870d-b57f-431c-af13-14d854ce5530" 14, "71c232c2-f111-44a4-98f7-09202435f3d2" 403329, "d666d2ca-1623-4d45-836c-587b7f13f0a5" 19095, "3e9fa2df-b618-4f6c-b6e2-7cdebfbad440" 244177, "8eb14cbe-a99e-41aa-854d-4bb20358b7d3" 8654, "036f0f46-eb02-480b-9c27-3c682dcc0b5d" 25671994, "a977a4c6-ca6e-4758-a004-761bb4f53d6f" 6275, "9a6f2284-7329-4639-809e-3af538301bb5" 9753, "170a9f44-eef5-48a7-85ae-b806cf62c1fc" 377621, "0f96100c-f08f-4745-88a2-bb4d45093bce" 146975, "1bb0f1f6-9035-4c11-b8b4-6a6c48bdac17" 4408544, "1f41b585-18da-4486-b31a-c59b0fda26eb" 35115468, "3c3da05e-9a20-4b8d-be9e-8787d26f05db" 24238, "4146ed83-2cb6-4cad-86bc-a390d3a872fa" 1210586, "9777d60d-a47a-4b31-9106-48c97914f337" 72467, "9553b7a8-3c25-4710-a11b-0355bce3549c" 27315, "e1118499-d0f7-4eb4-a837-dc84e86fcc6b" 53502, "f6a2f29d-d88b-43d0-b9f9-e43d91c2841c" 1479970892, "eda614e5-c67f-4bae-a4d8-6feddaa3ad3d" 371733, "94ff1963-69c8-4db1-9ef9-9629ec6b9d6b" 27831, "9039988a-3f2b-4faf-bcc7-5bff33775191" 4520018, "63b1dbcd-311c-4651-b124-692909ff6756" 28997, "e3da1f68-9a50-431a-a1c2-758f545d99fe" 8488455, "1ef046c1-f06a-4a42-b601-55cbe6439a82" 137662, "90e79f3c-c538-4869-98a6-76c75b1823ac" 415, "f38cb866-d28c-410b-b657-666cc763974c" 34759, "7d414c31-5a3c-473e-b5f6-ecdcc8cc6064" 4467100, "59538b58-eeae-4f3e-81c3-bbe3eb70f835" 56971, "0ec2b345-6e97-46d2-bbd1-95da8885c113" 28901973, "a4f9693f-93c3-4472-a494-99c6c0d6fbf9" 6390, "c7661366-31df-4525-9b59-7763ec0166a9" 11182, "7a69b94e-3a47-4b53-b92c-e1d130fc61b5" 339468, "ef8490eb-9382-44ed-949d-a67b89f68de9" 9277, "b796f6db-b532-43d0-a868-e3d6c686a7b2" 1589635, "2d008862-290f-4df4-a624-6e54b9c880f4" 369417, "1a901e0e-95bb-4a6a-9a08-8c9e47b325b1" 59612, "7d9ce909-5290-4340-9f7d-4fb633ea37fc" 38772982, "0cae2cef-e759-40bd-bba0-7c9ada31227b" 26965, "828c36a2-dace-4267-96cb-2fa346fda89c" 16354, "88aa39d8-e99b-43d9-a053-e5e0696712f5" 27294, "eb082d2c-f666-430a-8c41-3bc83781d3bb" 63155, "54839703-4042-4fab-974f-d6ad2a2dc4a3" 394783, "dd890fcc-db7f-4ba2-b732-6b9156afd7be" 893815, "62a4e3ca-726d-4e3a-919d-dbf69fd1d8a4" 23112, "1b5869a4-e00f-4f9d-8046-587ed0043439" 5426, "7122ad49-e0e0-4275-b265-c3544de653a9" 391824, "f7c00aa5-f198-475a-9f4b-2a01c38b1471" 61905, "291c4c1f-8f1e-4af9-9ade-3a242d935e5a" 11230, "3d2f22a6-9e20-4d23-910b-92e6a42296a8" 10073, "3dfb2ecc-24c7-413e-849f-301340010b98" 16126, "3d66a31c-3a08-43d8-a92e-41dd66238032" 62133, "288d84be-193d-419a-ae15-f6e6e550db8e" 128581632, "63d2b69e-b21c-421d-89c7-0215ecab8711" 21052, "f9978690-c6b6-4b77-8b6b-17130cf9cb5a" 2670003, "08920765-737a-4bcc-92e4-225964dd1f1d" 59799, "3155f8eb-7b17-465b-b30c-0cd71d722c23" 53270, "ad42f783-1f98-4381-b810-6e9083d0f00f" 40995, "34613d59-2fae-4cac-b165-17cd9841d361" 9089, "073a7e17-336a-41a8-ad22-b7dd40e6be42" 16212})

(defn bad-size?
  [event]
  (= :FIX-BYTE-COUNT
     (or (get-in event [:event :kixi.comms.event/payload :kixi.datastore.metadatastore/size-bytes])
         (get-in event [:event :kixi.comms.event/payload :kixi.datastore.metadatastore/file-metadata :kixi.datastore.metadatastore/size-bytes]))))

(defn fix-size
  [event]
  (if (= :kixi.datastore.file/created
         (:kixi.comms.event/key event))
    (assoc-in event
              [:kixi.comms.event/payload :kixi.datastore.metadatastore/size-bytes]
              (get file-id->size
                   (get-in event [:kixi.comms.event/payload :kixi.datastore.metadatastore/id])
                   :error))
    (assoc-in event
              [:kixi.comms.event/payload :kixi.datastore.metadatastore/file-metadata :kixi.datastore.metadatastore/size-bytes]
              (get file-id->size
                   (get-in event [:kixi.comms.event/payload :kixi.datastore.metadatastore/file-metadata :kixi.datastore.metadatastore/id])
                   :error))))

(defn correct-file-size
  [event]
  (if (bad-size? event)
    (update event :event fix-size)
    event))
