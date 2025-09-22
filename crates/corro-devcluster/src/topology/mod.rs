use nom::{
    bytes::complete::tag,
    character::complete::{alpha1, multispace0},
    sequence::delimited,
    IResult, Parser,
};
use std::collections::{BTreeMap, BTreeSet};

/// Topology definition as a set of graph edges
#[derive(Debug, Default)]
pub struct Simple {
    pub(crate) inner: BTreeMap<String, Vec<String>>,
}

impl Simple {
    /// Parse a topology line in the following format:
    ///
    /// A -> B
    /// B -> C
    /// A -> C
    /// etc
    pub fn parse_edge<'input>(&mut self, input: &'input str) -> IResult<&'input str, ()> {
        let (input, first) = alpha1(input)?;
        let (input, _) = delimited(multispace0, tag("->"), multispace0).parse(input)?;
        let (input, second) = alpha1(input)?;

        // Add first -> second edge
        self.inner
            .entry(first.to_string())
            .or_default()
            .push(second.to_string());
        // Add second to the map if it doesn't exist yet but don't
        // create the connection edge
        self.inner.entry(second.to_string()).or_default();

        Ok((input, ()))
    }

    pub fn get_all_nodes(&self) -> Vec<String> {
        self.inner
            .keys()
            .fold(BTreeSet::new(), |mut acc, key| {
                acc.insert(key.clone());
                self.inner.get(key).unwrap().iter().for_each(|value| {
                    acc.insert(value.clone());
                });
                acc
            })
            .into_iter()
            .collect()
    }
}
