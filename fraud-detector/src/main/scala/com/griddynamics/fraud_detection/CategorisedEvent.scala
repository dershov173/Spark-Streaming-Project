package com.griddynamics.fraud_detection

case class CategorisedEvent(eventType: String,
                            ipAddress: String,
                            eventTime: String,
                            url: String,
                            isBot: Boolean)

