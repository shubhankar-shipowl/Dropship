# Gmail Labels - How They Work

## Understanding Gmail Labels

Gmail labels are like **tags** or **folders** - they organize your emails but don't merge them.

### How Labels Work:

1. **Each email is separate** - Even if two emails have the same label, they remain as separate emails
2. **Label view shows all emails** - When you click on a label, you see ALL emails with that label in a list
3. **Emails can have multiple labels** - An email can have both "Dropshipper" and "Important" labels

### Example:

If you send 2 emails to the same person and both have the "Dropshipper" label:
- ✅ Both emails will appear when you click on "Dropshipper" label
- ✅ They will appear as 2 separate emails in the list
- ❌ They will NOT merge into one email

## Why You See Two Separate Emails

If you see two emails with the same subject:
1. **They were sent separately** - Each "Send Email" click creates a new email
2. **Both should have the label** - If both were sent via Gmail API, both should have the "Dropshipper" label
3. **They appear separately in the label** - This is normal Gmail behavior

## Checking if Labels Are Applied

To verify both emails have the label:

1. **In Gmail:**
   - Click on the "Dropshipper" label in the left sidebar
   - You should see both emails listed
   - Each email should show the label badge

2. **Using Debug Script:**
   ```bash
   node debug-gmail-labels.js
   ```
   This will show which labels are applied to recent emails

## If Emails Don't Appear in Label

If emails are not appearing in the label:

1. **Check if Gmail API was used:**
   - Look at server logs when sending
   - Should see: `📧 Using Gmail API to send email with label...`
   - If you see: `📧 Using SMTP to send email...` - labels won't work

2. **Check label application:**
   - Server logs should show: `✅ "Dropshipper" label applied successfully`
   - If you see errors, the label wasn't applied

3. **Verify in Gmail:**
   - Go to Gmail → Click "Dropshipper" label
   - Both emails should appear in the list

## Threading vs Labels

**Gmail Threading:**
- Gmail automatically groups emails with the same subject in a conversation
- This is different from labels
- Threading groups emails, labels organize them

**If you want emails grouped:**
- Gmail automatically threads emails with the same subject
- They appear as a conversation thread
- But in label view, they still appear as separate items

## Summary

- ✅ **Normal behavior:** Multiple emails with the same label appear as separate items
- ✅ **Both emails should have the label** if sent via Gmail API
- ✅ **They appear together** when you click on the label
- ❌ **They don't merge** - each email stays separate

If you want to verify both emails have the label, check the "Dropshipper" label view in Gmail - both should appear there.

