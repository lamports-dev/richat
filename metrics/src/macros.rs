// Copied from https://github.com/metrics-rs/metrics/blob/main/metrics/src/macros.rs
// To use local recorder instead of global one

#[macro_export]
macro_rules! metadata_var {
    ($target:expr, $level:expr) => {{
        static METADATA: $crate::Metadata<'static> = $crate::Metadata::new(
            $target,
            $level,
            ::core::option::Option::Some(::std::module_path!()),
        );
        &METADATA
    }};
}

#[macro_export]
macro_rules! count {
    () => {
        0usize
    };
    ($head:tt $($tail:tt)*) => {
        1usize + $crate::count!($($tail)*)
    };
}

#[macro_export]
macro_rules! key_var {
    ($name: literal) => {{
        static METRIC_KEY: $crate::Key = $crate::Key::from_static_name($name);
        &METRIC_KEY
    }};
    ($name:expr) => {
        $crate::Key::from_name($name)
    };
    ($name:literal, $($label_key:literal => $label_value:literal),*) => {{
        static LABELS: [$crate::Label; $crate::count!($($label_key)*)] = [
            $($crate::Label::from_static_parts($label_key, $label_value)),*
        ];
        static METRIC_KEY: $crate::Key = $crate::Key::from_static_parts($name, &LABELS);
        &METRIC_KEY
    }};
    ($name:expr, $($label_key:literal => $label_value:literal),*) => {{
        static LABELS: [$crate::Label; $crate::count!($($label_key)*)] = [
            $($crate::Label::from_static_parts($label_key, $label_value)),*
        ];
        $crate::Key::from_static_labels($name, &LABELS)
    }};
    ($name:expr, $($label_key:expr => $label_value:expr),*) => {{
        let labels = ::std::vec![
            $($crate::Label::new($label_key, $label_value)),*
        ];
        $crate::Key::from_parts($name, labels)
    }};
    ($name:expr, $labels:expr) => {
        $crate::Key::from_parts($name, $labels)
    }
}

#[macro_export]
macro_rules! counter {
    (recorder: $recorder:expr, target: $target:expr, level: $level:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {{
        #[allow(unused_imports)]
        use $crate::Recorder;

        let metric_key = $crate::key_var!($name $(, $label_key $(=> $label_value)?)*);
        let metadata = $crate::metadata_var!($target, $level);

        $recorder.register_counter(&metric_key, metadata)
    }};
    (recorder: $recorder:expr, target: $target:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {
        $crate::counter!(recorder: $recorder, target: $target, level: $crate::Level::INFO, $name $(, $label_key $(=> $label_value)?)*)
    };
    (recorder: $recorder:expr, level: $level:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {
        $crate::counter!(recorder: $recorder, target: ::std::module_path!(), level: $level, $name $(, $label_key $(=> $label_value)?)*)
    };
    ($recorder:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {
        $crate::counter!(recorder: $recorder, target: ::std::module_path!(), level: $crate::Level::INFO, $name $(, $label_key $(=> $label_value)?)*)
    };
}

#[macro_export]
macro_rules! gauge {
    (recorder: $recorder:expr, target: $target:expr, level: $level:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {{
        #[allow(unused_imports)]
        use $crate::Recorder;

        let metric_key = $crate::key_var!($name $(, $label_key $(=> $label_value)?)*);
        let metadata = $crate::metadata_var!($target, $level);

        $recorder.register_gauge(&metric_key, metadata)
    }};
    (recorder: $recorder:expr, target: $target:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {
        $crate::gauge!(recorder: $recorder, target: $target, level: $crate::Level::INFO, $name $(, $label_key $(=> $label_value)?)*)
    };
    (recorder: $recorder:expr, level: $level:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {
        $crate::gauge!(recorder: $recorder, target: ::std::module_path!(), level: $level, $name $(, $label_key $(=> $label_value)?)*)
    };
    ($recorder:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {
        $crate::gauge!(recorder: $recorder, target: ::std::module_path!(), level: $crate::Level::INFO, $name $(, $label_key $(=> $label_value)?)*)
    };
}

#[macro_export]
macro_rules! histogram {
    (recorder: $recorder:expr, target: $target:expr, level: $level:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {{
        #[allow(unused_imports)]
        use $crate::Recorder;

        let metric_key = $crate::key_var!($name $(, $label_key $(=> $label_value)?)*);
        let metadata = $crate::metadata_var!($target, $level);

        $recorder.register_histogram(&metric_key, metadata)
    }};
    (recorder: $recorder:expr, target: $target:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {
        $crate::histogram!(recorder: $recorder, target: $target, level: $crate::Level::INFO, $name $(, $label_key $(=> $label_value)?)*)
    };
    (recorder: $recorder:expr, level: $level:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {
        $crate::histogram!(recorder: $recorder, target: ::std::module_path!(), level: $level, $name $(, $label_key $(=> $label_value)?)*)
    };
    ($recorder:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {
        $crate::histogram!(recorder: $recorder, target: ::std::module_path!(), level: $crate::Level::INFO, $name $(, $label_key $(=> $label_value)?)*)
    };
}

#[macro_export]
macro_rules! describe {
    ($recorder:expr, $method:ident, $name:expr, $unit:expr, $description:expr $(,)?) => {{
        #[allow(unused_imports)]
        use $crate::Recorder;
        $recorder.$method(
            ::core::convert::Into::into($name),
            ::core::option::Option::Some($unit),
            ::core::convert::Into::into($description),
        );
    }};
    ($recorder:expr, $method:ident, $name:expr, $description:expr $(,)?) => {{
        #[allow(unused_imports)]
        use $crate::Recorder;
        $recorder.$method(
            ::core::convert::Into::into($name),
            ::core::option::Option::None,
            ::core::convert::Into::into($description),
        );
    }};
}

#[macro_export]
macro_rules! describe_counter {
    ($recorder:expr, $name:expr, $unit:expr, $description:expr $(,)?) => {
        $crate::describe!($recorder, describe_counter, $name, $unit, $description)
    };
    ($recorder:expr, $name:expr, $description:expr $(,)?) => {
        $crate::describe!($recorder, describe_counter, $name, $description)
    };
}

#[macro_export]
macro_rules! describe_gauge {
    ($recorder:expr, $name:expr, $unit:expr, $description:expr $(,)?) => {
        $crate::describe!($recorder, describe_gauge, $name, $unit, $description)
    };
    ($recorder:expr, $name:expr, $description:expr $(,)?) => {
        $crate::describe!($recorder, describe_gauge, $name, $description)
    };
}

#[macro_export]
macro_rules! describe_histogram {
    ($recorder:expr, $name:expr, $unit:expr, $description:expr $(,)?) => {
        $crate::describe!($recorder, describe_histogram, $name, $unit, $description)
    };
    ($recorder:expr, $name:expr, $description:expr $(,)?) => {
        $crate::describe!($recorder, describe_histogram, $name, $description)
    };
}

#[cfg(test)]
mod tests {
    use ::metrics::{NoopRecorder, Unit};

    static NOOP_RECORDER: NoopRecorder = NoopRecorder;

    #[test]
    fn test_counter() {
        describe_counter!(NOOP_RECORDER, "some_metric_name", "my favorite counter");
        describe_counter!(
            NOOP_RECORDER,
            "some_metric_name",
            Unit::Bytes,
            "my favorite counter"
        );

        counter!(NOOP_RECORDER, "some_metric_name", "service" => "http").absolute(42);
    }

    #[test]
    fn test_gauge() {
        describe_gauge!(NOOP_RECORDER, "some_metric_name", "my favorite gauge");
        describe_gauge!(
            NOOP_RECORDER,
            "some_metric_name",
            Unit::Bytes,
            "my favorite gauge"
        );

        gauge!(NOOP_RECORDER, "some_metric_name", "service" => "http").set(42.0);
    }

    #[test]
    fn test_histogram() {
        describe_histogram!(NOOP_RECORDER, "some_metric_name", "my favorite histogram");
        describe_histogram!(
            NOOP_RECORDER,
            "some_metric_name",
            Unit::Bytes,
            "my favorite histogram"
        );

        histogram!(NOOP_RECORDER, "some_metric_name", "service" => "http").record(1.0);
    }
}
