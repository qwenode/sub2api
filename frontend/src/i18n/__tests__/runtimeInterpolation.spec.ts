import { beforeAll, describe, expect, it, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import UsageProgressBar from '@/components/account/UsageProgressBar.vue'
import { i18n, loadLocaleMessages } from '../index'

beforeAll(async () => {
  await Promise.all([loadLocaleMessages('zh'), loadLocaleMessages('en')])
})

describe.each(['zh', 'en'] as const)('%s runtime interpolation', (locale) => {
  it('formats expiry, currency and zero counts without dynamic code evaluation', () => {
    const dynamicEval = vi.spyOn(globalThis, 'eval').mockImplementation(() => {
      throw new Error('CSP blocks dynamic code evaluation')
    })
    const dynamicFunction = vi.spyOn(globalThis, 'Function').mockImplementation(() => {
      throw new Error('CSP blocks dynamic code evaluation')
    })

    try {
      const time = '11/30 18:00'
      const expiry = locale === 'zh' ? `到期 ${time}` : `Expires ${time}`
      for (const section of ['openaiQuotaReset', 'claudeResetCredits']) {
        expect(i18n.global.t(`admin.accounts.${section}.expiresAt`, { time }, { locale }))
          .toBe(expiry)
      }
      expect(i18n.global.t('admin.accounts.usageWindow.estimatedTotalCost', { cost: '123.45' }, { locale }))
        .toBe(locale === 'zh' ? '预计总费用 $123.45' : 'Est. total $123.45')
      expect(i18n.global.t('admin.accounts.usageWindow.estimatedTotalCost', { cost: '0.00' }, { locale }))
        .toBe(locale === 'zh' ? '预计总费用 $0.00' : 'Est. total $0.00')
      expect(i18n.global.t('admin.accounts.claudeResetCredits.confirmMessage', { windows: '5h', count: 0 }, { locale }))
        .toBe(locale === 'zh'
          ? '将消耗 1 次重置次数，立即恢复 5h 窗口，剩余 0 次。此操作不可撤销，确定继续吗？'
          : 'This will consume 1 reset credit to immediately restore the 5h window(s) (0 remaining). This action cannot be undone. Continue?')
      expect(dynamicEval).not.toHaveBeenCalled()
      expect(dynamicFunction).not.toHaveBeenCalled()
    } finally {
      dynamicEval.mockRestore()
      dynamicFunction.mockRestore()
    }
  })

  it('renders the estimated cost badge with the application locale messages', async () => {
    i18n.global.locale.value = locale
    const wrapper = mount(UsageProgressBar, {
      global: { plugins: [i18n] },
      props: {
        label: '7d',
        utilization: 50,
        color: 'emerald',
        windowStats: { requests: 1, tokens: 100, cost: 10 },
        estimatedTotalCost: 20
      }
    })

    try {
      expect(wrapper.get('[data-test="estimated-total-cost"]').text())
        .toBe(locale === 'zh' ? '预计总费用 $20.00' : 'Est. total $20.00')
      await wrapper.setProps({ estimatedTotalCost: 0 })
      expect(wrapper.get('[data-test="estimated-total-cost"]').text())
        .toBe(locale === 'zh' ? '预计总费用 $0.00' : 'Est. total $0.00')
    } finally {
      wrapper.unmount()
    }
  })
})
