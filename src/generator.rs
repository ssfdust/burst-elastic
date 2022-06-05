use fake::Dummy;
use fake::{Fake, Faker};
use fake::faker::lorem::zh_cn::Sentence;
use fake::uuid::UUIDv4;
use property::Property;
use serde::{Deserialize, Serialize};

#[derive(Debug, Dummy, Property, Serialize, Clone)]
pub struct FakeData {
    #[dummy(faker = "UUIDv4")]
    pub id: String,

    #[dummy(faker = "Sentence(100..102)")]
    pub body: String
}

pub fn init_fake_data(num: usize) -> Vec<FakeData> {
    fake::vec![FakeData as Faker; num]
}
