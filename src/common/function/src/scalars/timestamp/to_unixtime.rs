#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(unused_imports)]

use std::fmt;
use std::sync::Arc;

use common_query::error::{
    ArrowComputeSnafu, IntoVectorSnafu, Result, TypeCastSnafu, UnsupportedInputDataTypeSnafu,
};
use common_query::prelude::{Signature, Volatility};
use datatypes::arrow::compute;
use datatypes::arrow::datatypes::{DataType as ArrowDatatype, Int64Type};
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;
use datatypes::vectors::{TimestampMillisecondVector, VectorRef};
use snafu::ResultExt;

use crate::scalars::function::{Function, FunctionContext};


#[derive(Clone, Debug, Default)]
pub struct ToUnixtimeFuntion;

const NAME: &str = "to_unixtime";

impl Function for ToUnixtimeFuntion {
    fn name(&self) -> &str {
        "to_unixtime"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::timestamp_millisecond_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::uniform(
            1,
            vec![ConcreteDataType::int64_datatype()],
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: FunctionContext, _columns: &[VectorRef]) -> Result<VectorRef> {
        todo!()
    }
}

impl fmt::Display for ToUnixtimeFuntion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TO_UNIXTIME")
    }
}

#[cfg(test)]

mod tests {
  use common_query::prelude::TypeSignature;
  use datafusion::arrow::datatypes::UInt8Type;
use datatypes::value::Value;
  use datatypes::vectors::{Int64Vector, UInt8Vector};

  use super::*;
  
  #[test]
  fn test_to_unixtime() {
    let f = ToUnixtimeFuntion::default();
    assert_eq!("to_unixtime", f.name());
    assert_eq!(
        ConcreteDataType::timestamp_millisecond_datatype(),
        f.return_type(&[]).unwrap()
    );

    assert!(matches!(f.signature(),
                      Signature {
                          type_signature: TypeSignature::Uniform(1, valid_types),
                          volatility: Volatility::Immutable
                      } if  valid_types == vec![ConcreteDataType::int64_datatype()]
    ));

    let times = vec![Some("2023-03-01T06:35:02Z".to_string())];
    // let args: Vec<VectorRef> = vec![Arc::new(times.clone())];
    // let vector = f.eval(FunctionContext::default(), &args).unwrap();
  }
}
