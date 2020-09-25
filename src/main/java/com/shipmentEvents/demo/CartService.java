package com.babelcentral.bridge.service.commerce;

import com.babelcentral.bridge.dao.commerce.AccountCartDao;
import com.babelcentral.bridge.db.dbAccountCartEntity;
import com.babelcentral.bridge.db.dbAccountEntity;
import com.babelcentral.bridge.dto.AccountCartDto;
import com.babelcentral.bridge.messaging.MessageGateway;
import com.babelcentral.bridge.service.identity.AccountService;
import com.babelcentral.bridge.util.Constants;
import com.babelcentral.services.element.commerce.CartStatus;
import com.babelcentral.services.element.identity.feature.FeatureKey;
import com.babelcentral.services.parameters.SendAbandonedCartRecipient;
import com.babelcentral.services.persistence.sql.Transactional;
import com.babelcentral.services.util.FeatureUtil;
import com.babelcentral.services.util.HysterixWrapper;
import com.google.common.collect.Lists;
import com.newrelic.api.agent.Trace;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;


public class CartService {

    private Logger logger = LogManager.getLogger(CartService.class);

    @Inject
    private AccountCartDao accountCartDao;

    @Inject
    private AccountService accountService;

    @Trace
    @Transactional
    public void updateAccountCart(int accountId, int productId) {
        dbAccountCartEntity entity = accountCartDao.get(accountId);
        if (entity == null)
            entity = new dbAccountCartEntity(accountId, productId, CartStatus.NEW.getValue());
        else {
            entity.setDateModified(new Date());
            entity.setProductId(productId);
            entity.setCartStatus(CartStatus.NEW.getValue());
        }
        accountCartDao.saveOrUpdate(entity);
    }


    @Trace
    public void sendAbandonedCartEmails(Map<String, Object> params) {
        List<AccountCartDto> accountCartDtos = getAccountCartDtosByParams(params);
        Set<Integer> recipientAccountIds = new HashSet<>();
        Set<Integer> subscribedAccountIds = new HashSet<>();

        accountCartDtos.forEach(currentDto -> {
            if (currentDto.getSubscriptionCount() == 0) {
                recipientAccountIds.add(currentDto.getAccountId());
            } else {
                subscribedAccountIds.add(currentDto.getAccountId());
            }
        });

        if (CollectionUtils.isNotEmpty(subscribedAccountIds))
            updateCartStatus(subscribedAccountIds, CartStatus.SUBSCRIBED.getValue());

        if (CollectionUtils.isEmpty(recipientAccountIds))
            return;

        callPOAbandonedCart(recipientAccountIds);
    }

    @Trace
    private void callPOAbandonedCart(Set<Integer> recipientAccountIds) {

        List<List<Integer>> batchedAccounts = Lists.partition(Lists.newArrayList(recipientAccountIds), Constants.MAX_ITEM_SIZE);
        batchedAccounts.forEach(batch -> {
            List<SendAbandonedCartRecipient> abandonedCartRecipients = getAbandonedCartRecipients(new HashSet<>(batch));
            sendAbandonedCartMessageAsync(abandonedCartRecipients);
        });
    }

    @Trace
    private void sendAbandonedCartMessageAsync(List<SendAbandonedCartRecipient> recipientList) {
        new HysterixWrapper<Boolean>(
                true, // enable: Boolean,
                true, // useThreads: Boolean,
                "CartService.sendAbandonedCartMessageAsync", // name: String,
                5, // maxConcurrentRequests: Int,
                500, // circuitBreakerRequestVolumeThreshold: Int,
                50, // circuitBreakerErrorThresholdPercentage: Int,
                5000, // circuitBreakerSleepWindowInMilliseconds: Int,
                5000 // executionTimeoutInMilliseconds: Int
        ) {
            public Boolean body() {
                Set<Integer> recipientAccountIds = new HashSet<>();
                recipientList.forEach(recipient -> recipientAccountIds.add(recipient.getAccountId()));
                try {
                    MessageGateway.sendAbandonedCartAsServer(recipientList);
                    updateCartStatus(recipientAccountIds, CartStatus.MAIL_SENT_SUCCESS.getValue());
                } catch (Exception e) {
                    updateCartStatus(recipientAccountIds, CartStatus.MAIL_SENT_FAILED.getValue());
                    logger.error("Failure while trying to send SendAbandonedCartRecipients to PO", e);
                }
                return true;
            }
        }.runAsync();
    }

    @Trace
    @Transactional
    private void updateCartStatus(Set<Integer> accountIds, String cartStatus) {
        accountCartDao.updateCartStatus(accountIds, cartStatus);
    }

    @Trace
    private List<SendAbandonedCartRecipient> getAbandonedCartRecipients(Set<Integer> accountIds) {
        List<dbAccountEntity> accountEntities = accountService.getAccountsById(accountIds);
        Map<Integer, String> accountToVariantMap = FeatureUtil.getInternalFeatureStringForAccounts(FeatureKey.PO_MESSAGE_VARIANT, accountIds);
        return accountEntities.stream().map(account -> createSendAbandonedCartRecipient(
                account,
                accountToVariantMap.get(account.getAccountId()))
        ).collect(Collectors.toList());
    }

    @Trace
    private SendAbandonedCartRecipient createSendAbandonedCartRecipient(dbAccountEntity account, String variant) {
        return new SendAbandonedCartRecipient(account.getAccountId(), account.getName(), account.getEmail(), account.getSiteLanguage(), variant);
    }

    @Trace
    @Transactional
    private List<AccountCartDto> getAccountCartDtosByParams(Map<String, Object> params) {
        return accountCartDao.getAccountCartDtosByParams(params);
    }
}
