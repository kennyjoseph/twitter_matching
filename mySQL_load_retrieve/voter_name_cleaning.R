
# In voterfile, names contain: hyphen ("HEDIN-JONES"), period ("st. john"), comma ("mccall, jr."),
# space ("VAN GIESEN"), apostrophe ("O'CONNOR"; "FORTE'").
# Where it's present, in our sample, reg_address_state always matches state_code.

# Expanding to NH data: now we have accent grave ("ANN`"), backslash (OUELLETTE\\), 
# underscore ("CJ_"), slash ("FIELD/TORREY") and weirdness ("WHITTEN (82)")
# Also name alternatives (or clarifications?) within parentheses: 
# "C V (CHARLES VINCENT)", "JAMES (SON)", "EUGENE (GENE)"
# IA data: percent signs ("ZE%"), bracket ("[HILLIP"), semicolon ("DO;;EU")
# And wow, what a lot of numbers accidentally in the mix (the whole time). Substitutions 
# ("BARTOL0MEI", "WHITTALL 1V"), typos ("JANIC3E"), and random nonsense ("WILLIAM03041997", "KENNEALLY6").

# Call this on first name and last name separately.
getNameWordsFromVoter = function(voterName) {
    
    # delete apostrophes; change periods, commas and hyphens to spaces
    name = gsub("\\'", "", voterName, perl=T)
    name = gsub("[\\-\\.\\,]", " ", name, perl=T)
    # other new things to change to spaces: accent grave, underscore, back & forward slashes, 
    # percent sign, bracket, semicolon
    name = gsub("[\\`_/\\\\;%\\[]", " ", name, perl=T)
    # delete anything within parens
    name = gsub("\\(.*\\)", "", name, perl=T)
    
    # substitute 1 --> I and 0 --> 0
    name = gsub("1", "I", name, perl=T)
    name = gsub("0", "O", name, perl=T)
    # delete other numbers
    name = gsub("\\d", "", name, perl=T)
    
    # warn about any new weird punctuation we meet
    if (grepl("[^A-Za-z\\s]", name, perl=T)) {
        warning(paste("odd new punctuation removed from this voter name: ", name))
        name = gsub("[^A-Za-z\\s]", "", name, perl=T)
    }
    
    # fix special cases that are inconsistent in voter file:
    # "O CONNOR" --> "O'CONNOR" --> "OCONNOR" [after deleting apostrophe]
    # "MC GEE" --> "MCGEE" ("MC" or "MAC" is not a full word in a last name)
    name = gsub("\\bO\\s+", "O", name, perl=T)
    name = gsub("\\bMC\\s+", "MC", name, perl=T)
    name = gsub("\\bMAC\\s+", "MAC", name, perl=T)
    
    # trim leading whitespace before splitting
    name = sub("^\\s+", "", name, perl=T)
    words = strsplit(name, "\\s+", perl=T)[[1]]
    
    return(tolower(words))
}

