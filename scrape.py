import request

# go to site
# look inside sidebar
# loop from 2 to 15
    # find div with id text-{number}
    # for each table row in the table
        # save the href of each link the second cell
        # spawn a thread to go to the link
        # get the href inside anchor tag inside paragraph above div with class "audioplayer"