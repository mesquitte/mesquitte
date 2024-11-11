//! QoS (Quality of Services)

use std::fmt::Display;

#[repr(u8)]
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone)]
pub enum QualityOfService {
    Level0 = 0,
    Level1 = 1,
    Level2 = 2,
}

impl From<QoSWithPacketIdentifier> for QualityOfService {
    fn from(qos: QoSWithPacketIdentifier) -> Self {
        match qos {
            QoSWithPacketIdentifier::Level0 => QualityOfService::Level0,
            QoSWithPacketIdentifier::Level1(_) => QualityOfService::Level1,
            QoSWithPacketIdentifier::Level2(_) => QualityOfService::Level2,
        }
    }
}

impl Display for QualityOfService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", *self as u8)
    }
}

/// QoS with identifier pairs
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone)]
pub enum QoSWithPacketIdentifier {
    Level0,
    Level1(u16),
    Level2(u16),
}

impl QoSWithPacketIdentifier {
    pub fn new(qos: QualityOfService, id: u16) -> Self {
        match (qos, id) {
            (QualityOfService::Level0, _) => QoSWithPacketIdentifier::Level0,
            (QualityOfService::Level1, id) => QoSWithPacketIdentifier::Level1(id),
            (QualityOfService::Level2, id) => QoSWithPacketIdentifier::Level2(id),
        }
    }

    pub fn split(self) -> (QualityOfService, Option<u16>) {
        match self {
            QoSWithPacketIdentifier::Level0 => (QualityOfService::Level0, None),
            QoSWithPacketIdentifier::Level1(pkid) => (QualityOfService::Level1, Some(pkid)),
            QoSWithPacketIdentifier::Level2(pkid) => (QualityOfService::Level2, Some(pkid)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::cmp::min;

    #[test]
    fn min_qos() {
        let q1 = QoSWithPacketIdentifier::Level1(0).into();
        let q2 = QualityOfService::Level2;
        assert_eq!(min(q1, q2), q1);

        let q1 = QoSWithPacketIdentifier::Level0.into();
        let q2 = QualityOfService::Level2;
        assert_eq!(min(q1, q2), q1);

        let q1 = QoSWithPacketIdentifier::Level2(0).into();
        let q2 = QualityOfService::Level1;
        assert_eq!(min(q1, q2), q2);
    }
}
