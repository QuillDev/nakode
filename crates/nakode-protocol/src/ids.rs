use std::fmt;

use serde::{Deserialize, Serialize};

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl $name {
            #[must_use]
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }
    };
}

id_type!(AgentSessionId);
id_type!(ArtifactId);
id_type!(ClientId);
id_type!(EntryId);
id_type!(IdempotencyKey);
id_type!(InteractionId);
id_type!(ModelId);
id_type!(PromptId);
id_type!(ProviderId);
id_type!(RequestId);
id_type!(RunId);
id_type!(ServerEpoch);
id_type!(SessionId);
id_type!(SubscriptionId);
id_type!(TurnId);
id_type!(WorkspaceId);
