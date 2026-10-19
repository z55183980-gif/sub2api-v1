import { afterEach, expect, it, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import type { Account } from '@/types'
import AccountCapacityCell from '../AccountCapacityCell.vue'
import en from '@/i18n/locales/en'

vi.mock('vue-i18n', () => ({ useI18n: () => ({ t: (key: string) => key.split('.').reduce((value: any, part) => value?.[part], en) ?? key }) }))

afterEach(() => vi.useRealTimers())

it('shows zero HTTP requests alongside five waiting tool processes and expires both observations', async () => {
  vi.useFakeTimers()
  vi.setSystemTime(new Date('2026-10-19T04:00:00Z'))
  const account = {
    platform: 'anthropic', type: 'apikey', concurrency: 5, current_concurrency: 0,
    concurrency_observed_at: new Date().toISOString(),
    wrapper_capacity: { valid: true, observed_at: new Date().toISOString(), generating: 0, waiting_tools: 5,
      idle_retained: 0, queued: 0, total_processes: 5, available: 0, limit: 5, draining: false }
  } as Account
  const wrapper = mount(AccountCapacityCell, { props: { account } })
  expect(wrapper.text()).toContain('0/5HTTP')
  expect(wrapper.text()).toContain('Waiting tools 5')
  expect(wrapper.text()).toContain('Total processes 5')
  await vi.advanceTimersByTimeAsync(31000)
  expect(wrapper.text()).toContain('—/5HTTP')
  expect(wrapper.text()).toContain('Process state unavailable or stale')
  expect(wrapper.text()).not.toContain('Waiting tools 5')
  wrapper.unmount()
})

it('does not substitute zero when collecting concurrency failed', () => {
  const account = { platform: 'anthropic', type: 'apikey', concurrency: 5, current_concurrency: 0,
    concurrency_observed_at: null, extra: { wrapper_protocol: 'anthropic-messages-cli-v1' }
  } as Account
  const wrapper = mount(AccountCapacityCell, { props: { account } })
  expect(wrapper.text()).toContain('—/5HTTP')
  expect(wrapper.text()).toContain('Process state unavailable or stale')
  wrapper.unmount()
})
