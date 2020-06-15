package com.pkaufmann.kotlin.ddd.application.domain

import arrow.Kind

typealias Send<T, F> = (T) -> Kind<F, Unit>