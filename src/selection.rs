use rand::Rng;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WeightFill {
    MissingEqualsOne,
    MissingLessOrEqualOne,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChooseDetail {
    pub index: usize,
    pub weights: Vec<f64>,
    pub total_weight: f64,
    pub random_value: Option<f64>,
    pub used_uniform_fallback: bool,
}

pub fn choose_weighted_index<F>(len: usize, raw_weight: F, fill: WeightFill) -> ChooseDetail
where
    F: FnMut(usize) -> f64,
{
    choose_weighted_index_with_rng(len, raw_weight, fill, rand::thread_rng())
}

fn choose_weighted_index_with_rng<F, R>(
    len: usize,
    raw_weight: F,
    fill: WeightFill,
    mut rng: R,
) -> ChooseDetail
where
    F: FnMut(usize) -> f64,
    R: Rng,
{
    assert!(
        len > 0,
        "weighted selection requires at least one candidate"
    );

    let raw: Vec<f64> = (0..len).map(raw_weight).collect();
    let mut weights = raw.clone();
    let mut data_sum = 0.0;
    let mut data_count = 0usize;

    for &weight in &raw {
        if has_observed_data(weight, fill) {
            data_sum += weight;
            data_count += 1;
        }
    }

    if data_count > 0 {
        let avg_weight = data_sum / data_count as f64;
        for weight in &mut weights {
            if should_fill_missing(*weight, fill) {
                *weight = avg_weight;
            }
        }
    }

    let total_weight: f64 = weights.iter().sum();
    if total_weight <= 0.0 {
        return ChooseDetail {
            index: rng.gen_range(0..len),
            weights,
            total_weight,
            random_value: None,
            used_uniform_fallback: true,
        };
    }

    let random_value = rng.gen::<f64>() * total_weight;
    let mut cumulative = 0.0;
    let index = weights
        .iter()
        .position(|weight| {
            cumulative += *weight;
            cumulative >= random_value
        })
        .unwrap_or(0);

    ChooseDetail {
        index,
        weights,
        total_weight,
        random_value: Some(random_value),
        used_uniform_fallback: false,
    }
}

fn has_observed_data(weight: f64, fill: WeightFill) -> bool {
    match fill {
        WeightFill::MissingEqualsOne => weight != 1.0,
        WeightFill::MissingLessOrEqualOne => weight > 1.0,
    }
}

fn should_fill_missing(weight: f64, fill: WeightFill) -> bool {
    match fill {
        WeightFill::MissingEqualsOne => weight == 1.0,
        WeightFill::MissingLessOrEqualOne => weight <= 1.0,
    }
}

#[cfg(test)]
mod tests {
    use super::{choose_weighted_index_with_rng, WeightFill};
    use rand::rngs::mock::StepRng;

    #[test]
    fn fills_missing_equals_one_with_average_and_uses_weighted_pick() {
        let detail = choose_weighted_index_with_rng(
            3,
            |idx| [10.0, 1.0, 20.0][idx],
            WeightFill::MissingEqualsOne,
            mock_f64_rng(0.5),
        );

        assert_eq!(detail.weights, vec![10.0, 15.0, 20.0]);
        assert_eq!(detail.total_weight, 45.0);
        assert_eq!(detail.random_value, Some(22.5));
        assert_eq!(detail.index, 1);
        assert!(!detail.used_uniform_fallback);
    }

    #[test]
    fn falls_back_to_uniform_when_total_weight_is_not_positive() {
        let detail = choose_weighted_index_with_rng(
            3,
            |idx| [0.0, 0.0, 0.0][idx],
            WeightFill::MissingEqualsOne,
            StepRng::new(u64::MAX / 2, 0),
        );

        assert_eq!(detail.weights, vec![0.0, 0.0, 0.0]);
        assert_eq!(detail.total_weight, 0.0);
        assert_eq!(detail.random_value, None);
        assert!(detail.used_uniform_fallback);
        assert!(detail.index < 3);
    }

    #[test]
    fn fills_missing_less_or_equal_one_with_average() {
        let detail = choose_weighted_index_with_rng(
            3,
            |idx| [1.0, 8.0, 0.5][idx],
            WeightFill::MissingLessOrEqualOne,
            mock_f64_rng(0.2),
        );

        assert_eq!(detail.weights, vec![8.0, 8.0, 8.0]);
        assert_eq!(detail.total_weight, 24.0);
        assert!((detail.random_value.expect("expected random value") - 4.8).abs() < 1e-12);
        assert_eq!(detail.index, 0);
        assert!(!detail.used_uniform_fallback);
    }

    fn mock_f64_rng(value: f64) -> StepRng {
        assert!((0.0..1.0).contains(&value));
        StepRng::new((value * u64::MAX as f64) as u64, 0)
    }
}
