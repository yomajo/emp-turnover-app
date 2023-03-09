
// Close notifications
document.addEventListener('DOMContentLoaded', () => {
  (document.querySelectorAll('.notification .delete') || []).forEach(($delete) => {
    const $notification = $delete.parentNode

    $delete.addEventListener('click', () => {
      $notification.parentNode.removeChild($notification)
    })
  })
})


function alertOnDuration(e) {
  msg = `Background processing / threading is not implemented.
    This may or may not take a while. Do not close.
    See progress in app.log inside project directory`
  console.log(e)
  if (confirm(msg)) {
    console.log(e.currentTarget.getAttribute("href"))
    window.location = e.currentTarget.getAttribute("href")
  } else {
    console.log(e)
    
    e.preventDefault()
  }
}
