#![feature(step_trait)]

use std::{
    fmt,
    iter::Step,
    ops::{Add, Sub},
};

use rangemap::StepLite;
use rusqlite::{types::FromSql, ToSql};
use serde::{Deserialize, Serialize};
use speedy::{Context, Readable, Writable};

#[derive(
    Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Version(pub u64);

impl From<Version> for u64 {
    fn from(v: Version) -> Self {
        v.0
    }
}

impl Step for Version {
    fn steps_between(start: &Self, end: &Self) -> Option<usize> {
        u64::steps_between(&start.0, &end.0)
    }

    fn forward_checked(start: Self, count: usize) -> Option<Self> {
        u64::forward_checked(start.0, count).map(Self)
    }

    fn backward_checked(start: Self, count: usize) -> Option<Self> {
        u64::backward_checked(start.0, count).map(Self)
    }
}

impl StepLite for Version {
    fn add_one(&self) -> Self {
        Self(self.0 + 1)
    }

    fn sub_one(&self) -> Self {
        Self(self.0 - 1)
    }
}

impl ToSql for Version {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        self.0.to_sql()
    }
}

impl FromSql for Version {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        u64::column_result(value).map(Self)
    }
}

impl Add<u64> for Version {
    type Output = Self;

    fn add(self, rhs: u64) -> Self::Output {
        Self(self.0.add(rhs))
    }
}

impl Sub<u64> for Version {
    type Output = Self;

    fn sub(self, rhs: u64) -> Self::Output {
        Self(self.0.sub(rhs))
    }
}

impl<'a, C> Readable<'a, C> for Version
where
    C: Context,
{
    fn read_from<R: speedy::Reader<'a, C>>(reader: &mut R) -> Result<Self, <C as Context>::Error> {
        u64::read_from(reader).map(Self)
    }
}

impl<C> Writable<C> for Version
where
    C: Context,
{
    fn write_to<T: ?Sized + speedy::Writer<C>>(
        &self,
        writer: &mut T,
    ) -> Result<(), <C as Context>::Error> {
        self.0.write_to(writer)
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(
    Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct CrsqlDbVersion(pub u64);

impl Step for CrsqlDbVersion {
    fn steps_between(start: &Self, end: &Self) -> Option<usize> {
        u64::steps_between(&start.0, &end.0)
    }

    fn forward_checked(start: Self, count: usize) -> Option<Self> {
        u64::forward_checked(start.0, count).map(Self)
    }

    fn backward_checked(start: Self, count: usize) -> Option<Self> {
        u64::backward_checked(start.0, count).map(Self)
    }
}

impl StepLite for CrsqlDbVersion {
    fn add_one(&self) -> Self {
        Self(self.0 + 1)
    }

    fn sub_one(&self) -> Self {
        Self(self.0 - 1)
    }
}

impl ToSql for CrsqlDbVersion {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        self.0.to_sql()
    }
}

impl FromSql for CrsqlDbVersion {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        u64::column_result(value).map(Self)
    }
}

impl Add<u64> for CrsqlDbVersion {
    type Output = Self;

    fn add(self, rhs: u64) -> Self::Output {
        Self(self.0.add(rhs))
    }
}

impl Sub<u64> for CrsqlDbVersion {
    type Output = Self;

    fn sub(self, rhs: u64) -> Self::Output {
        Self(self.0.sub(rhs))
    }
}

impl<'a, C> Readable<'a, C> for CrsqlDbVersion
where
    C: Context,
{
    fn read_from<R: speedy::Reader<'a, C>>(reader: &mut R) -> Result<Self, <C as Context>::Error> {
        u64::read_from(reader).map(Self)
    }
}

impl<C> Writable<C> for CrsqlDbVersion
where
    C: Context,
{
    fn write_to<T: ?Sized + speedy::Writer<C>>(
        &self,
        writer: &mut T,
    ) -> Result<(), <C as Context>::Error> {
        self.0.write_to(writer)
    }
}

impl fmt::Display for CrsqlDbVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(
    Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct CrsqlSeq(pub u64);

impl Step for CrsqlSeq {
    fn steps_between(start: &Self, end: &Self) -> Option<usize> {
        u64::steps_between(&start.0, &end.0)
    }

    fn forward_checked(start: Self, count: usize) -> Option<Self> {
        u64::forward_checked(start.0, count).map(Self)
    }

    fn backward_checked(start: Self, count: usize) -> Option<Self> {
        u64::backward_checked(start.0, count).map(Self)
    }
}

impl StepLite for CrsqlSeq {
    fn add_one(&self) -> Self {
        Self(self.0 + 1)
    }

    fn sub_one(&self) -> Self {
        Self(self.0 - 1)
    }
}

impl ToSql for CrsqlSeq {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        self.0.to_sql()
    }
}

impl FromSql for CrsqlSeq {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        u64::column_result(value).map(Self)
    }
}

impl Add<u64> for CrsqlSeq {
    type Output = Self;

    fn add(self, rhs: u64) -> Self::Output {
        Self(self.0.add(rhs))
    }
}

impl Sub<u64> for CrsqlSeq {
    type Output = Self;

    fn sub(self, rhs: u64) -> Self::Output {
        Self(self.0.sub(rhs))
    }
}

impl<'a, C> Readable<'a, C> for CrsqlSeq
where
    C: Context,
{
    fn read_from<R: speedy::Reader<'a, C>>(reader: &mut R) -> Result<Self, <C as Context>::Error> {
        u64::read_from(reader).map(Self)
    }
}

impl<C> Writable<C> for CrsqlSeq
where
    C: Context,
{
    fn write_to<T: ?Sized + speedy::Writer<C>>(
        &self,
        writer: &mut T,
    ) -> Result<(), <C as Context>::Error> {
        self.0.write_to(writer)
    }
}

impl fmt::Display for CrsqlSeq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
