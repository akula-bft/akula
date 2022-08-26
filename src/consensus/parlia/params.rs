// Copyright 2015-2020 Parity Technologies (UK) Ltd.
// This file is part of OpenEthereum.

// OpenEthereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// OpenEthereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with OpenEthereum.  If not, see <http://www.gnu.org/licenses/>.

//! Parlia specific parameters.

use ethjson;

/// `Parlia` params.
pub struct ParliaParams {
    /// Number of seconds between blocks to enforce
    pub period: u64,
    /// Epoch length to update validatorSet
    pub epoch: u64,
}

impl From<ethjson::spec::ParliaParams> for ParliaParams {
    fn from(p: ethjson::spec::ParliaParams) -> Self {
        let period = p.period.map_or_else(|| 30000 as u64, Into::into);
        let epoch = p.epoch.map_or_else(|| 15 as u64, Into::into);

        assert!(epoch > 0);
        assert!(period > 0);

        ParliaParams { period, epoch }
    }
}
