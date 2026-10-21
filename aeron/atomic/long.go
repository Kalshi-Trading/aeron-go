/*
Copyright 2016 Stanislav Liberman

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package atomic

import "sync/atomic"

type Long struct {
	value atomic.Int64 // 64-bit aligned on 32-bit platforms
}

func (i *Long) Get() int64 {
	return i.value.Load()
}

func (i *Long) Set(v int64) {
	i.value.Store(v)
}

func (i *Long) Add(delta int64) int64 {
	return i.value.Add(delta)
}

func (i *Long) Inc() int64 {
	return i.Add(1)
}
