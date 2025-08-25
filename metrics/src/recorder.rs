use {
    ::metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit},
    std::fmt,
};

pub enum MaybeRecorder<R> {
    Recorder(R),
    Noop,
}

impl<R> fmt::Debug for MaybeRecorder<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Recorder(_) => write!(f, "MaybeRecorder::Recorder(_)"),
            Self::Noop => write!(f, "MaybeRecorder::Noop"),
        }
    }
}

impl<R> From<R> for MaybeRecorder<R> {
    fn from(value: R) -> Self {
        Self::Recorder(value)
    }
}

impl<R: Recorder> Recorder for MaybeRecorder<R> {
    fn describe_counter(&self, key_name: KeyName, unit: Option<Unit>, description: SharedString) {
        match self {
            Self::Recorder(recorder) => recorder.describe_counter(key_name, unit, description),
            Self::Noop => {}
        }
    }

    fn describe_gauge(&self, key_name: KeyName, unit: Option<Unit>, description: SharedString) {
        match self {
            Self::Recorder(recorder) => recorder.describe_gauge(key_name, unit, description),
            Self::Noop => {}
        }
    }

    fn describe_histogram(&self, key_name: KeyName, unit: Option<Unit>, description: SharedString) {
        match self {
            Self::Recorder(recorder) => recorder.describe_histogram(key_name, unit, description),
            Self::Noop => {}
        }
    }

    fn register_counter(&self, key: &Key, metadata: &Metadata<'_>) -> Counter {
        match self {
            Self::Recorder(recorder) => recorder.register_counter(key, metadata),
            Self::Noop => Counter::noop(),
        }
    }

    fn register_gauge(&self, key: &Key, metadata: &Metadata<'_>) -> Gauge {
        match self {
            Self::Recorder(recorder) => recorder.register_gauge(key, metadata),
            Self::Noop => Gauge::noop(),
        }
    }

    fn register_histogram(&self, key: &Key, metadata: &Metadata<'_>) -> Histogram {
        match self {
            Self::Recorder(recorder) => recorder.register_histogram(key, metadata),
            Self::Noop => Histogram::noop(),
        }
    }
}
