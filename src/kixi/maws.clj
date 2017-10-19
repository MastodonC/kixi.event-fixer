(ns kixi.maws
  (:require [aero.core :as aero]
            [amazonica.aws.securitytoken :as sts]
            [amazonica.core :as amazonica :refer [with-credential]]))

(defn federated-config []
  (-> (System/getProperty "user.home")
      (str "/.aws/etc/client.edn")
      (aero/read-config)))

(defn get-credentials [config account type & mfa]
  (let [{:keys [user trusted-profile trusted-account-id
                trusted-role-readonly trusted-role-admin account-ids]} config
        trusted-role (case type
                       "ro" trusted-role-readonly
                       "admin" trusted-role-admin
                       :else (println (str "Unknown role type: " type)))
        account-id (account-ids (keyword account))
        role-arn (str "arn:aws:iam::" account-id ":role/" trusted-role)
        mfa-device-serial-number (str "arn:aws:iam::" trusted-account-id ":mfa/" user)
        mfa-token (first mfa)
        ar (with-credential {:profile trusted-profile}
             (if mfa-token
               (sts/assume-role :role-arn role-arn :role-session-name account
                                :serial-number mfa-device-serial-number :token-code mfa-token)
               (sts/assume-role :role-arn role-arn :role-session-name account)))
        credentials (ar :credentials)]
    credentials))

(defn witan-prod-creds
  []
  (-> (federated-config)
      (get-credentials :witan-prod "ro")))

(defn witan-admin-prod-creds
  [mfa]
  (-> (federated-config)
      (get-credentials :witan-prod "admin" mfa)))
